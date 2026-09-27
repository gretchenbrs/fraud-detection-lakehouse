"""Isolated rolling validation experiments; never read the 2019 test period."""

from __future__ import annotations

from datetime import date
from functools import reduce

from src.features.point_in_time import (
    STATIC_NUMERIC, SAFE_CATEGORICAL, HISTORY_NUMERIC, ENCODING_NUMERIC,
    SNAPSHOT_NUMERIC, add_history_features, add_delayed_mcc_encoding,
)
from src.models.features import prepare_model_input, fit_training_preprocessor
from src.models.training import fit_logistic_regression
from src.models.evaluation import (
    score_predictions, overall_auc_row, threshold_metrics,
    select_best_validation_threshold, top_k_capture_rows,
)
from src.utils.config import silver_dataset_runtime

FOLDS = (
    ("2017", "2016-12-31", "2017-12-31"),
    ("2018", "2017-12-31", "2018-12-31"),
)
CANDIDATES = (
    ("static_moderate", STATIC_NUMERIC, 0.5),
    ("history_moderate", STATIC_NUMERIC + HISTORY_NUMERIC, 0.5),
    ("history_mcc_moderate", STATIC_NUMERIC + HISTORY_NUMERIC + ENCODING_NUMERIC, 0.5),
    ("history_mcc_unweighted", STATIC_NUMERIC + HISTORY_NUMERIC + ENCODING_NUMERIC, 0.0),
    ("history_mcc_balanced", STATIC_NUMERIC + HISTORY_NUMERIC + ENCODING_NUMERIC, 1.0),
    ("snapshot_ablation", STATIC_NUMERIC + HISTORY_NUMERIC + ENCODING_NUMERIC + SNAPSHOT_NUMERIC, 0.5),
)
LABEL_DELAY_DAYS = 30


def experimental_weights(positive_count, negative_count, power):
    """Interpolate class weighting while keeping mean training weight equal to one."""
    if min(positive_count, negative_count) <= 0 or not 0 <= power <= 1:
        raise ValueError("Both classes and weight power in [0, 1] are required.")
    ratio = (negative_count / positive_count) ** power
    scale = (positive_count + negative_count) / (negative_count + positive_count * ratio)
    return ratio * scale, scale


def validate_fold(train_end, validation_end):
    if not date.fromisoformat(train_end) < date.fromisoformat(validation_end) < date(2019, 1, 1):
        raise ValueError("Experimental folds must be chronological and strictly before 2019.")


def _table(config, suffix):
    from src.models.lifecycle import artifact_locations
    artifact_locations(config)  # Validate SQL identifiers before constructing names.
    namespace = config["databricks"]
    return f"{namespace['catalog_name']}.{namespace['schema_name']}.experiment_v2_{suffix}"


def _write(frame, name, mode="overwrite"):
    writer = frame.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(name)


def prepare_experiment(spark, config):
    """Pin a Silver version and materialize all-label history before filtering labels."""
    from pyspark.sql import functions as F

    runtime = silver_dataset_runtime(config, "transactions")
    if runtime["storage_mode"] != "unity_catalog":
        raise ValueError("V2 experiment requires versioned Unity Catalog Silver tables.")
    source = runtime["target_table"]
    version = spark.sql(f"DESCRIBE HISTORY {source} LIMIT 1").first()["version"]
    rows = spark.read.option("versionAsOf", version).table(source).filter(
        F.col("transaction_date") < F.lit("2019-01-01").cast("date")
    )
    if rows.filter(F.col("transaction_timestamp").isNull()).limit(1).count():
        raise ValueError("History requires valid transaction timestamps.")
    features = add_history_features(rows)
    _write(features, _table(config, "history"))
    audit = {
        "source_table": source, "source_version": int(version),
        "label_delay_days": LABEL_DELAY_DAYS,
        "test_period_read": False, "evaluation": "rolling_validation_only",
    }
    _write(spark.createDataFrame([audit]), _table(config, "input_audit"))
    return audit


def run_fold(spark, config, fold_name):
    """Compare feature and weight ablations on one untouched chronological validation year."""
    from pyspark.sql import functions as F

    matches = [fold for fold in FOLDS if fold[0] == fold_name]
    if not matches:
        raise ValueError(f"Unknown fold: {fold_name}")
    _, train_end, validation_end = matches[0]
    validate_fold(train_end, validation_end)
    history = spark.table(_table(config, "history")).filter(
        F.col("transaction_date") <= F.lit(validation_end).cast("date")
    )
    encoded = add_delayed_mcc_encoding(history, train_end, LABEL_DELAY_DAYS)
    encoded = encoded.filter(F.col("is_fraud").isNotNull()).withColumn(
        "data_split",
        F.when(F.col("transaction_date") <= F.lit(train_end).cast("date"), "train").otherwise("validation"),
    )
    feature_table = _table(config, f"features_{fold_name}")
    _write(encoded, feature_table)
    data = spark.table(feature_table)
    # Model fitting itself must respect the assumed label availability delay.
    training_rows = data.filter(
        (F.col("data_split") == "train")
        & (F.col("transaction_date") <= F.date_sub(F.lit(train_end).cast("date"), LABEL_DELAY_DAYS))
    )
    validation_rows = data.filter(F.col("data_split") == "validation")
    counts = {int(r["is_fraud"]): int(r["count"]) for r in training_rows.groupBy("is_fraud").count().collect()}
    validation_count = validation_rows.count()
    if validation_count == 0:
        raise ValueError("Validation split is empty.")

    all_rows = []
    for index, (name, numeric, power) in enumerate(CANDIDATES):
        positive_weight, negative_weight = experimental_weights(counts.get(1, 0), counts.get(0, 0), power)
        train = prepare_model_input(training_rows, numeric, SAFE_CATEGORICAL).withColumn(
            "class_weight", F.when(F.col("label") == 1, positive_weight).otherwise(negative_weight)
        )
        validation = prepare_model_input(validation_rows, numeric, SAFE_CATEGORICAL)
        print(f"[v2:{fold_name}] fitting {name}, class_weight_power={power}", flush=True)
        preprocessor = fit_training_preprocessor(train, numeric, SAFE_CATEGORICAL)
        model = fit_logistic_regression(preprocessor.transform(train), config["models"],
            {"max_iter": 60, "reg_param": 0.05, "elastic_net_param": 0.0})
        predictions = score_predictions(model, preprocessor.transform(validation), name)
        score_table = _table(config, f"scores_{fold_name}_{name}")
        _write(predictions, score_table)
        scores = spark.table(score_table)
        metrics = overall_auc_row(scores, name, "validation", num_bins=0)
        metrics.update({
            "fold": fold_name, "train_end": train_end, "validation_end": validation_end,
            "weight_power": power, "label_delay_days": LABEL_DELAY_DAYS,
            "uses_snapshot_features": name == "snapshot_ablation",
            "pr_auc_num_bins": 0, "training_rows": sum(counts.values()),
        })
        quantiles = scores.approxQuantile("score", [0.95, 0.975, 0.99, 0.995, 0.999], 0.0001)
        thresholds = sorted(set(
            value for value in [*config["models"]["thresholds"], *quantiles] if 0 < value < 1
        ))
        grid = threshold_metrics(spark, scores, name, "validation", thresholds)
        best = select_best_validation_threshold(grid).first().asDict()
        metrics.update({f"selected_{key}": float(best[key]) for key in ("threshold", "precision", "recall", "f1")})
        for row in top_k_capture_rows(spark, scores, name, "validation", [0.001, 0.005, 0.01]):
            suffix = str(row["top_fraction"]).replace(".", "_")
            metrics[f"precision_at_{suffix}"] = row["precision_at_k"]
            metrics[f"recall_at_{suffix}"] = row["fraud_capture_rate"]
        if metrics["row_count"] != validation_count:
            raise ValueError("Candidate changed the validation population.")
        all_rows.append(metrics)
        _write(spark.createDataFrame(all_rows), _table(config, f"metrics_{fold_name}"))
        _write(grid.withColumn("fold", F.lit(fold_name)), _table(config, f"thresholds_{fold_name}_{name}"))
        print(f"[v2:{fold_name}] {name}: PR-AUC={metrics['pr_auc']:.6f}", flush=True)
    return all_rows


def summarize_experiment(spark, config):
    """Rank by rolling validation only; report snapshot ablation but never promote it."""
    from pyspark.sql import functions as F

    all_metrics = reduce(lambda a, b: a.unionByName(b), [
        spark.table(_table(config, f"metrics_{fold}")) for fold, _, _ in FOLDS
    ])
    leaderboard = all_metrics.groupBy("model_name", "uses_snapshot_features").agg(
        F.countDistinct("fold").alias("fold_count"),
        F.avg("pr_auc").alias("mean_validation_pr_auc"),
        F.min("pr_auc").alias("worst_validation_pr_auc"),
        F.avg("recall_at_0_01").alias("mean_recall_at_one_percent"),
        F.avg("precision_at_0_01").alias("mean_precision_at_one_percent"),
    ).orderBy(F.desc("mean_validation_pr_auc"))
    if leaderboard.filter(F.col("fold_count") != len(FOLDS)).count():
        raise ValueError("An experiment candidate is missing a validation fold.")
    _write(leaderboard, _table(config, "leaderboard"))
    return [row.asDict() for row in leaderboard.collect()]


def audit_legacy_validation(spark, config):
    """Re-evaluate existing 2018 baseline scores with the same exact metric definition."""
    from pyspark.sql import functions as F
    from src.models.pipeline import load_model_dataset

    scores = load_model_dataset(spark, config, "scored_predictions").filter(
        (F.col("data_split") == "validation")
        & (F.col("transaction_date") >= F.lit("2018-01-01").cast("date"))
        & (F.col("transaction_date") < F.lit("2019-01-01").cast("date"))
        & (F.col("model_name") == "logistic_regression_baseline")
    )
    if not scores.limit(1).count():
        raise ValueError("Original tuning baseline validation scores are unavailable.")
    result = overall_auc_row(scores, "logistic_regression_baseline", "validation", num_bins=0)
    result["pr_auc_num_bins"] = 0
    result["fold"] = "2018"
    for row in top_k_capture_rows(spark, scores, "logistic_regression_baseline", "validation", [0.01]):
        result["recall_at_one_percent"] = row["fraud_capture_rate"]
        result["precision_at_one_percent"] = row["precision_at_k"]
    _write(spark.createDataFrame([result]), _table(config, "legacy_reference"))
    return result
