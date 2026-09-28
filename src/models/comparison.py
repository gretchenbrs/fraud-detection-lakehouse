"""Fixed model-family comparison followed by a frozen 2019 historical evaluation."""

import json
import math
from collections import defaultdict

from src.features.point_in_time import (
    STATIC_NUMERIC, HISTORY_NUMERIC, ENCODING_NUMERIC, SAFE_CATEGORICAL,
    add_history_features, add_delayed_mcc_encoding,
)
from src.models.experiments import FOLDS, LABEL_DELAY_DAYS, _table, _write
from src.models.features import prepare_model_input, fit_training_preprocessor
from src.models.training import add_training_class_weights, fit_logistic_regression, fit_random_forest
from src.models.evaluation import score_predictions, overall_auc_row, top_k_capture_rows, threshold_metrics

NUMERIC = STATIC_NUMERIC + HISTORY_NUMERIC + ENCODING_NUMERIC
FAMILIES = ("logistic_regression", "random_forest", "gradient_boosted_trees")
SETTINGS = {
    "logistic_regression": {"max_iter": 60, "reg_param": 0.05, "elastic_net_param": 0.0},
    "random_forest": {"num_trees": 80, "max_depth": 8, "max_bins": 64, "subsampling_rate": 0.8},
    "gradient_boosted_trees": {"max_iter": 40, "max_depth": 5, "max_bins": 64, "step_size": 0.1},
}


def table(config, name):
    return _table(config, "family_" + name)


def artifact_path(config, family, fold, run_tag):
    if family not in FAMILIES or fold not in ("2017", "2018") or not run_tag.isdigit():
        raise ValueError("Invalid artifact identity.")
    table(config, "audit")
    ns = config["databricks"]
    return f"/Volumes/{ns['catalog_name']}/{ns['schema_name']}/model_artifacts/family_{run_tag}/{fold}/{family}"


def rank_candidates(records):
    grouped = defaultdict(dict)
    for record in records:
        family, fold = record["model_name"], record["fold"]
        if family not in FAMILIES or fold not in ("2017", "2018") or fold in grouped[family]:
            raise ValueError("Unexpected or duplicate validation result.")
        for key in ("pr_auc", "recall_at_one_percent"):
            if not math.isfinite(record[key]) or not 0 <= record[key] <= 1:
                raise ValueError("Invalid validation metric.")
        grouped[family][fold] = record
    if set(grouped) != set(FAMILIES) or any(set(v) != {"2017", "2018"} for v in grouped.values()):
        raise ValueError("All families require both validation years.")
    ranked = []
    for family, folds in grouped.items():
        ranked.append({
            "model_name": family,
            "mean_recall_at_one_percent": sum(r["recall_at_one_percent"] for r in folds.values()) / 2,
            "mean_pr_auc": sum(r["pr_auc"] for r in folds.values()) / 2,
        })
    return sorted(ranked, key=lambda r: (-r["mean_recall_at_one_percent"], -r["mean_pr_auc"], r["model_name"]))


def fit_family(train, config, family):
    if family == "logistic_regression":
        return fit_logistic_regression(train, config["models"], SETTINGS[family])
    if family == "random_forest":
        return fit_random_forest(train, config["models"], SETTINGS[family])
    if family != "gradient_boosted_trees":
        raise ValueError(family)
    from pyspark.ml.classification import GBTClassifier
    hp = SETTINGS[family]
    return GBTClassifier(
        featuresCol="raw_features", labelCol="label", weightCol="class_weight",
        probabilityCol="probability", rawPredictionCol="raw_prediction",
        predictionCol="default_prediction", seed=int(config["models"]["seed"]),
        maxIter=hp["max_iter"], maxDepth=hp["max_depth"], maxBins=hp["max_bins"],
        stepSize=hp["step_size"], subsamplingRate=0.8,
    ).fit(train)


def metrics(spark, scores, family, split):
    result = overall_auc_row(scores, family, split, num_bins=0)
    top = top_k_capture_rows(spark, scores, family, split, [0.01])[0]
    result.update(recall_at_one_percent=top["fraud_capture_rate"], precision_at_one_percent=top["precision_at_k"])
    return result


def run_validation(spark, config, fold, family, run_tag):
    from pyspark.sql import functions as F
    from pyspark.ml import PipelineModel

    end = dict((f, end) for f, end, _ in FOLDS)[fold]
    data = spark.table(_table(config, f"features_{fold}"))
    train = data.filter((F.col("data_split") == "train") &
                        (F.col("transaction_date") <= F.date_sub(F.to_date(F.lit(end)), LABEL_DELAY_DAYS)))
    validation = data.filter(F.col("data_split") == "validation")
    train = add_training_class_weights(prepare_model_input(train, NUMERIC, SAFE_CATEGORICAL))
    validation = prepare_model_input(validation, NUMERIC, SAFE_CATEGORICAL)
    prep = fit_training_preprocessor(train, NUMERIC, SAFE_CATEGORICAL)
    fitted = fit_family(prep.transform(train), config, family)
    stages = fitted.stages if isinstance(fitted, PipelineModel) else [fitted]
    pipeline = PipelineModel(stages=[*prep.stages, *stages])
    path = artifact_path(config, family, fold, run_tag)
    pipeline.write().save(path)
    loaded = PipelineModel.load(path)
    sample = validation.orderBy("transaction_id").limit(100)
    before = score_predictions(pipeline, sample, family).select("transaction_id", "score")
    after = score_predictions(loaded, sample, family).select("transaction_id", "score")
    if before.exceptAll(after).limit(1).count():
        raise ValueError("Saved model replay differs.")
    _write(score_predictions(loaded, validation, family), table(config, f"scores_{fold}_{family}"))
    scores = spark.table(table(config, f"scores_{fold}_{family}"))
    result = metrics(spark, scores, family, "validation")
    result.update(fold=fold, run_tag=run_tag, artifact_path=path, hyperparameters=json.dumps(SETTINGS[family], sort_keys=True))
    _write(spark.createDataFrame([result]), table(config, f"metrics_{fold}_{family}"))
    return result


def freeze_selection(spark, config, run_tag):
    from pyspark.sql import functions as F

    records = [spark.table(table(config, f"metrics_{fold}_{family}")).first().asDict()
               for fold in ("2017", "2018") for family in FAMILIES]
    if any(r["run_tag"] != run_tag for r in records):
        raise ValueError("Mixed experiment runs.")
    for fold in ("2017", "2018"):
        populations = {(r["row_count"], r["fraud_count"]) for r in records if r["fold"] == fold}
        if len(populations) != 1:
            raise ValueError("Validation populations differ.")
    leaderboard = rank_candidates(records)
    family = leaderboard[0]["model_name"]
    scores = spark.table(table(config, f"scores_2018_{family}"))
    count = scores.count()
    # Score cutoff is selected without labels. Top-k remains a separate fixed-budget policy.
    threshold = scores.orderBy(F.desc("score"), F.asc("transaction_id")).limit(math.ceil(count * 0.01)).agg(F.min("score")).first()[0]
    audit = spark.table(_table(config, "input_audit")).first().asDict()
    selection = {
        "model_name": family, "run_tag": run_tag,
        "artifact_path": artifact_path(config, family, "2018", run_tag),
        "threshold": float(threshold), "review_fraction": 0.01,
        "source_table": audit["source_table"], "source_version": audit["source_version"],
        "train_end": "2017-12-31", "label_delay_days": LABEL_DELAY_DAYS,
        "selection_rule": "mean 2017/2018 recall at top 1%; tie-break mean PR-AUC, then name",
        "test_previously_inspected": True,
    }
    _write(spark.createDataFrame(leaderboard), table(config, "leaderboard"))
    _write(spark.createDataFrame([selection]), table(config, "selection"))
    return {"leaderboard": leaderboard, "selection": selection}


def evaluate_frozen_test(spark, config, run_tag):
    from pyspark.sql import functions as F
    from pyspark.ml import PipelineModel

    selected = spark.table(table(config, "selection")).first().asDict()
    if selected["run_tag"] != run_tag:
        raise ValueError("Selection is not from this run.")
    source = spark.read.option("versionAsOf", selected["source_version"]).table(selected["source_table"])
    source = source.filter(F.col("transaction_date") < F.lit("2020-01-01").cast("date"))
    features = add_delayed_mcc_encoding(add_history_features(source), selected["train_end"], LABEL_DELAY_DAYS)
    test = features.filter((F.col("transaction_date") >= F.lit("2019-01-01").cast("date")) & F.col("is_fraud").isNotNull())
    test = test.withColumn("data_split", F.lit("test"))
    _write(test, table(config, "test_features"))
    prepared = prepare_model_input(spark.table(table(config, "test_features")), NUMERIC, SAFE_CATEGORICAL)
    model = PipelineModel.load(selected["artifact_path"])
    family = selected["model_name"]
    _write(score_predictions(model, prepared, family), table(config, "test_scores"))
    scores = spark.table(table(config, "test_scores"))
    result = metrics(spark, scores, family, "test")
    result.update(run_tag=run_tag, test_previously_inspected=True, threshold=selected["threshold"])
    _write(spark.createDataFrame([result]), table(config, "test_metrics"))
    cutoff = threshold_metrics(spark, scores, family, "test", [selected["threshold"]])
    _write(cutoff, table(config, "test_threshold_metrics"))
    return {"metrics": result, "fixed_threshold": cutoff.first().asDict(), "selection": selected}
