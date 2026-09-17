"""Training-only class weighting and Spark ML model fitting."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


MODEL_NAMES = ("logistic_regression", "random_forest")


def balanced_class_weights(positive_count: int, negative_count: int) -> tuple[float, float]:
    """Return inverse-frequency weights with equal total weight per class."""
    if positive_count <= 0 or negative_count <= 0:
        raise ValueError("Both fraud and non-fraud rows are required for class weighting.")
    total_count = positive_count + negative_count
    positive_weight = total_count / (2.0 * positive_count)
    negative_weight = total_count / (2.0 * negative_count)
    return positive_weight, negative_weight


def add_training_class_weights(training: DataFrame, enabled: bool = True) -> DataFrame:
    """Add weights using training-label counts; no validation or test rows are involved."""
    from pyspark.sql import functions as F

    if not enabled:
        return training.withColumn("class_weight", F.lit(1.0))

    counts = {
        int(row["label"]): int(row["count"])
        for row in training.groupBy("label").count().collect()
    }
    positive_weight, negative_weight = balanced_class_weights(
        positive_count=counts.get(1, 0),
        negative_count=counts.get(0, 0),
    )
    return training.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, F.lit(positive_weight)).otherwise(
            F.lit(negative_weight)
        ),
    )


def fit_logistic_regression(training: DataFrame, model_config: dict[str, Any]):
    """Fit the regularized baseline model on preprocessed training rows."""
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import StandardScaler

    config = model_config["logistic_regression"]
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withMean=False,
        withStd=True,
    )
    classifier = LogisticRegression(
        featuresCol="scaled_features",
        labelCol="label",
        weightCol="class_weight",
        probabilityCol="probability",
        rawPredictionCol="raw_prediction",
        predictionCol="default_prediction",
        maxIter=int(config["max_iter"]),
        regParam=float(config["reg_param"]),
        elasticNetParam=float(config["elastic_net_param"]),
        standardization=False,
    )
    return Pipeline(stages=[scaler, classifier]).fit(training)


def fit_random_forest(training: DataFrame, model_config: dict[str, Any]):
    """Fit the tree-based comparison model on the same training rows."""
    from pyspark.ml.classification import RandomForestClassifier

    config = model_config["random_forest"]
    classifier = RandomForestClassifier(
        featuresCol="raw_features",
        labelCol="label",
        weightCol="class_weight",
        probabilityCol="probability",
        rawPredictionCol="raw_prediction",
        predictionCol="default_prediction",
        numTrees=int(config["num_trees"]),
        maxDepth=int(config["max_depth"]),
        maxBins=int(config["max_bins"]),
        subsamplingRate=float(config["subsampling_rate"]),
        seed=int(model_config["seed"]),
    )
    return classifier.fit(training)
