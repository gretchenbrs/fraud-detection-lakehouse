"""Point-in-time experimental features; original feature tables remain unchanged."""

from __future__ import annotations

STATIC_NUMERIC = (
    "amount", "amount_abs", "log_amount_abs", "is_negative_amount",
    "transaction_hour", "transaction_day_of_week", "is_weekend", "is_night",
    "is_online_transaction", "hour_sin", "hour_cos",
)
SAFE_CATEGORICAL = ("transaction_type", "merchant_location_category", "mcc_category")
HISTORY_NUMERIC = (
    "card_count_1h", "card_amount_1h", "card_count_24h", "card_amount_24h",
    "user_count_30d", "user_mean_amount_30d", "user_std_amount_30d",
    "seconds_since_previous", "log_amount_to_history", "amount_zscore_30d",
    "history_available", "is_new_merchant",
)
ENCODING_NUMERIC = ("past_mcc_fraud_rate", "past_mcc_unseen")
SNAPSHOT_NUMERIC = (
    "snapshot_credit_limit", "snapshot_yearly_income", "snapshot_total_debt",
    "snapshot_credit_score", "snapshot_num_credit_cards",
)


def add_history_features(transactions):
    """Use all past transactions, including unlabeled ones, excluding same-time peers."""
    import math
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rows = (
        transactions.withColumn("_seconds", F.col("transaction_timestamp").cast("long"))
        .withColumn("_user", F.coalesce(F.col("user_id"), F.concat(F.lit("missing:"), F.col("transaction_id"))))
        .withColumn("_card", F.coalesce(F.col("card_id"), F.concat(F.lit("missing:"), F.col("transaction_id"))))
        .withColumn("_merchant", F.coalesce(F.col("merchant_id"), F.concat(F.lit("missing:"), F.col("transaction_id"))))
    )
    card = Window.partitionBy("_user", "_card").orderBy("_seconds")
    user = Window.partitionBy("_user").orderBy("_seconds")
    past_user = user.rangeBetween(Window.unboundedPreceding, -1)
    user_30d = user.rangeBetween(-30 * 86400, -1)
    past_merchant = Window.partitionBy("_user", "_merchant").orderBy("_seconds").rangeBetween(
        Window.unboundedPreceding, -1
    )
    for name, seconds in (("1h", 3600), ("24h", 86400)):
        frame = card.rangeBetween(-seconds, -1)
        rows = rows.withColumn(f"card_count_{name}", F.count("*").over(frame).cast("double"))
        rows = rows.withColumn(f"card_amount_{name}", F.coalesce(F.sum("amount_abs").over(frame), F.lit(0.0)))
    rows = (
        rows.withColumn("user_count_30d", F.count("*").over(user_30d).cast("double"))
        .withColumn("user_mean_amount_30d", F.avg("amount_abs").over(user_30d))
        .withColumn("user_std_amount_30d", F.stddev_pop("amount_abs").over(user_30d))
        .withColumn("seconds_since_previous", F.col("_seconds") - F.max("_seconds").over(past_user))
        .withColumn("is_new_merchant", (F.count("*").over(past_merchant) == 0).cast("double"))
        .withColumn("history_available", (F.col("user_count_30d") > 0).cast("double"))
        .withColumn("log_amount_abs", F.log1p("amount_abs"))
        .withColumn("log_amount_to_history", F.log1p("amount_abs") - F.log1p("user_mean_amount_30d"))
        .withColumn("amount_zscore_30d", F.when(
            F.col("user_std_amount_30d") > 0,
            F.greatest(F.lit(-20.0), F.least(F.lit(20.0),
                (F.col("amount_abs") - F.col("user_mean_amount_30d")) / F.col("user_std_amount_30d")))
        ))
        .withColumn("hour_sin", F.sin(F.col("transaction_hour") * F.lit(2 * math.pi / 24)))
        .withColumn("hour_cos", F.cos(F.col("transaction_hour") * F.lit(2 * math.pi / 24)))
    )
    for source in ("credit_limit", "yearly_income", "total_debt", "credit_score", "num_credit_cards"):
        rows = rows.withColumn(f"snapshot_{source}", F.col(source).cast("double"))
    keep = [
        "transaction_id", "transaction_timestamp", "transaction_date", "is_fraud", "mcc",
        *STATIC_NUMERIC, *SAFE_CATEGORICAL, *HISTORY_NUMERIC, *SNAPSHOT_NUMERIC,
    ]
    return rows.select(*keep)


def add_delayed_mcc_encoding(rows, train_end, label_delay_days=30, alpha=100.0, cold_prior=0.001):
    """Past-only training encoding, frozen validation mapping, assumed label delay."""
    if label_delay_days < 0 or alpha <= 0 or not 0 <= cold_prior <= 1:
        raise ValueError("Invalid encoding delay, smoothing, or cold-start prior.")
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Clamp validation to the forecast origin so no validation labels enter the mapping.
    origin = F.date_add(F.to_date(F.lit(train_end)), 1)
    keyed = (
        rows.withColumn("_mcc", F.coalesce(F.col("mcc"), F.lit("__unknown__")))
        .withColumn("_day", F.datediff(F.least(F.col("transaction_date"), origin), F.lit("1970-01-01")))
        .withColumn("_eligible_label", F.when(
            F.col("transaction_date") <= F.to_date(F.lit(train_end)), F.col("is_fraud").cast("double")
        ))
    )
    daily = keyed.groupBy("_mcc", "_day").agg(
        F.count("_eligible_label").alias("_n"),
        F.coalesce(F.sum("_eligible_label"), F.lit(0.0)).alias("_p"),
    )
    past = Window.partitionBy("_mcc").orderBy("_day").rangeBetween(
        Window.unboundedPreceding, -label_delay_days - 1
    )
    mcc_history = daily.select(
        "_mcc", "_day", F.coalesce(F.sum("_n").over(past), F.lit(0)).alias("_past_n"),
        F.coalesce(F.sum("_p").over(past), F.lit(0.0)).alias("_past_p"),
    )
    global_days = daily.groupBy("_day").agg(F.sum("_n").alias("_n"), F.sum("_p").alias("_p"))
    global_past = Window.orderBy("_day").rangeBetween(Window.unboundedPreceding, -label_delay_days - 1)
    global_rates = (
        global_days.withColumn("_global_n", F.sum("_n").over(global_past))
        .withColumn("_global_p", F.sum("_p").over(global_past))
        .select("_day", F.when(F.col("_global_n") > 0, F.col("_global_p") / F.col("_global_n"))
                .otherwise(F.lit(cold_prior)).alias("_prior"))
    )
    return (
        keyed.join(mcc_history, ["_mcc", "_day"], "left").join(global_rates, "_day", "left")
        .withColumn("past_mcc_fraud_rate", (F.col("_past_p") + F.lit(alpha) * F.col("_prior")) /
                    (F.col("_past_n") + F.lit(alpha)))
        .withColumn("past_mcc_unseen", (F.col("_past_n") == 0).cast("double"))
        .drop("_mcc", "_day", "_eligible_label", "_past_n", "_past_p", "_prior")
    )


def run_spark_guardrail_checks(spark):
    """Tiny executable checks for chronology, peer exclusion and label isolation."""
    from datetime import datetime
    from pyspark.sql import functions as F

    schema = "transaction_id string, user_id string, card_id string, merchant_id string, transaction_timestamp timestamp, amount_abs double, is_fraud int, mcc string"
    raw = spark.createDataFrame([
        ("a", "u", "c", "m", datetime(2016, 1, 1, 0), 10.0, None, "1"),
        ("b", "u", "c", "m", datetime(2016, 1, 1, 0, 30), 20.0, 0, "1"),
        ("c", "u", "c", "n", datetime(2016, 1, 1, 0, 30), 30.0, 1, "1"),
        ("d", "u", "c", "n", datetime(2016, 2, 5, 0), 40.0, 1, "1"),
    ], schema)
    rows = raw.withColumn("transaction_date", F.to_date("transaction_timestamp"))
    for name in STATIC_NUMERIC:
        if name not in rows.columns:
            rows = rows.withColumn(name, F.lit(0.0))
    for name in SAFE_CATEGORICAL:
        rows = rows.withColumn(name, F.lit("sample"))
    for name in ("credit_limit", "yearly_income", "total_debt", "credit_score", "num_credit_cards"):
        rows = rows.withColumn(name, F.lit(100.0))
    features = add_history_features(rows)
    records = {r["transaction_id"]: r.asDict() for r in features.collect()}
    assert records["b"]["card_count_1h"] == 1, "Unlabeled history must count."
    assert records["c"]["card_count_1h"] == 1, "Same-time peers must be excluded."
    assert records["b"]["is_new_merchant"] == 0
    assert records["c"]["is_new_merchant"] == 1
    prefix = add_history_features(rows.filter(F.col("transaction_id") != "d"))
    assert prefix.select("transaction_id", *HISTORY_NUMERIC).exceptAll(
        features.filter(F.col("transaction_id") != "d").select("transaction_id", *HISTORY_NUMERIC)
    ).count() == 0, "Future rows must not alter previous features."

    encoded = add_delayed_mcc_encoding(features, "2016-01-31", 30)
    reference = {r["transaction_id"]: r["past_mcc_fraud_rate"] for r in encoded.collect()}
    changed = features.withColumn("is_fraud", F.when(F.col("transaction_id") == "d", 0).otherwise(F.col("is_fraud")))
    check = {r["transaction_id"]: r["past_mcc_fraud_rate"] for r in add_delayed_mcc_encoding(changed, "2016-01-31", 30).collect()}
    assert reference == check, "Validation labels must not affect encodings."
    assert reference["b"] == 0.001, "Same-day labels must not affect own features."
    assert reference["d"] == 0.5, "Only matured training labels may set the frozen prior."
    return {"history_and_encoding_checks": "passed"}
