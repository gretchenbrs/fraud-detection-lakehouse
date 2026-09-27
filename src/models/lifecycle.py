"""Persist the reviewed model and replay historical scoring without fitting."""

from __future__ import annotations

import json
import re
from pathlib import Path

from src.features.pipeline import load_feature_dataset
from src.models.evaluation import score_predictions
from src.models.features import fit_training_preprocessor, prepare_model_input
from src.models.training import add_training_class_weights, fit_logistic_regression

APPROVED_MODEL = "logistic_regression_baseline"
APPROVED_PARAMETERS = {"max_iter": 40, "reg_param": 0.05, "elastic_net_param": 0.0}
APPROVED_THRESHOLD = 0.98
RELEASE = "lr_baseline_20260927"


def artifact_locations(config):
    namespace = config["databricks"]
    catalog, schema = namespace["catalog_name"], namespace["schema_name"]
    for identifier in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError("Artifact catalog and schema must be simple SQL identifiers.")
    return (
        f"{catalog}.{schema}.model_artifacts",
        f"/Volumes/{catalog}/{schema}/model_artifacts/{RELEASE}",
        f"{catalog}.{schema}.model_batch_scores",
    )


def read_release_metadata(path):
    metadata = json.loads(Path(path, "release.json").read_text())
    if (
        metadata.get("release") != RELEASE
        or metadata.get("model_name") != APPROVED_MODEL
        or metadata.get("parameters") != APPROVED_PARAMETERS
        or metadata.get("threshold") != APPROVED_THRESHOLD
    ):
        raise ValueError("Saved release does not match the reviewed model specification.")
    return metadata


def save_reviewed_model(spark, config):
    """Fit the already selected parameters once; save all fitted preprocessing."""
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F

    volume, path, _ = artifact_locations(config)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")
    if Path(path, "release.json").exists():
        return read_release_metadata(path)

    prepared = prepare_model_input(load_feature_dataset(spark, config, "model_features"))
    training = add_training_class_weights(
        prepared.filter(F.col("data_split") == "train"),
        enabled=bool(config["models"]["use_class_weights"]),
    )
    preprocessor = fit_training_preprocessor(training)
    classifier = fit_logistic_regression(
        preprocessor.transform(training), config["models"], APPROVED_PARAMETERS
    )
    combined = PipelineModel(stages=[*preprocessor.stages, *classifier.stages])
    # Each approved release is immutable; interrupted saves require a new release ID.
    combined.write().save(f"{path}/pipeline")
    replay = prepared.filter(F.col("data_split") == "test")
    replay.write.format("delta").mode("error").save(f"{path}/replay_features")
    loaded = PipelineModel.load(f"{path}/pipeline")
    sample = replay.orderBy("transaction_id").limit(1000)
    before = score_predictions(combined, sample, APPROVED_MODEL).select(
        "transaction_id", F.col("score").alias("before")
    )
    after = score_predictions(loaded, sample, APPROVED_MODEL).select(
        "transaction_id", F.col("score").alias("after")
    )
    max_error = before.join(after, "transaction_id").agg(
        F.max(F.abs(F.col("before") - F.col("after"))).alias("error")
    ).first()["error"]
    if max_error is None or max_error > 1e-10:
        raise ValueError(f"Saved-model score parity failed: {max_error}")
    metadata = {
        "release": RELEASE,
        "model_name": APPROVED_MODEL,
        "parameters": APPROVED_PARAMETERS,
        "threshold": APPROVED_THRESHOLD,
        "selected_on": "validation",
        "selection_run_id": "473360204017919",
        "validation_pr_auc": 0.06395773722284717,
        "roundtrip_max_score_error": float(max_error),
        "replay_rows": replay.count(),
        "scope": "historical_test_replay",
    }
    Path(path, "release.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def replay_saved_model(spark, config):
    """Load frozen features and model; no fit call or label-dependent ranking."""
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F

    _, path, target = artifact_locations(config)
    metadata = read_release_metadata(path)
    pipeline = PipelineModel.load(f"{path}/pipeline")
    features = spark.read.format("delta").load(f"{path}/replay_features")
    scores = (
        score_predictions(pipeline, features, metadata["model_name"])
        .withColumn("model_release", F.lit(metadata["release"]))
        .withColumn("selected_threshold", F.lit(metadata["threshold"]))
        .withColumn("is_flagged", F.col("score") >= F.lit(metadata["threshold"]))
    )
    scores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    summary = spark.table(target).agg(
        F.count("*").alias("row_count"),
        F.countDistinct("transaction_id").alias("distinct_ids"),
        F.sum(F.col("is_flagged").cast("long")).alias("flagged_count"),
        F.sum((F.col("score").isNull() | ~F.col("score").between(0, 1)).cast("long")).alias("invalid_scores"),
    ).first().asDict()
    if summary["row_count"] != metadata["replay_rows"] or summary["invalid_scores"]:
        raise ValueError(f"Saved-model replay validation failed: {summary}")
    return {**summary, "model_release": metadata["release"], "target": target}

