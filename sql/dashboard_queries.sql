-- Financial Transaction Risk Analytics dashboard queries
-- Default namespace: workspace.fraud_detection
-- Run each numbered section as a separate Databricks SQL query.

-- 1. Champion model scorecard
SELECT
  model_name,
  evaluation_split,
  pr_auc,
  roc_auc,
  selected_threshold,
  fraud_rate,
  row_count
FROM workspace.fraud_detection.gold_model_scorecard
WHERE is_champion = true
ORDER BY evaluation_split;

-- 2. Executive operating KPIs
SELECT
  MAX(model_name) AS model_name,
  MAX(selected_threshold) AS selected_threshold,
  SUM(transaction_count) AS transaction_count,
  SUM(fraud_count) AS fraud_count,
  SUM(flagged_count) AS flagged_count,
  SUM(true_positive_count) / NULLIF(SUM(flagged_count), 0) AS precision,
  SUM(true_positive_count) / NULLIF(SUM(fraud_count), 0) AS recall,
  SUM(flagged_count) / NULLIF(SUM(transaction_count), 0) AS alert_rate,
  SUM(captured_fraud_amount) / NULLIF(SUM(fraud_amount), 0)
    AS fraud_amount_capture_rate
FROM workspace.fraud_detection.gold_daily_risk_kpis;

-- 3. Monthly risk trend
SELECT
  DATE_TRUNC('month', transaction_date) AS transaction_month,
  SUM(transaction_count) AS transaction_count,
  SUM(fraud_count) / NULLIF(SUM(transaction_count), 0) AS fraud_rate,
  SUM(flagged_count) / NULLIF(SUM(transaction_count), 0) AS alert_rate,
  SUM(true_positive_count) / NULLIF(SUM(flagged_count), 0) AS precision,
  SUM(true_positive_count) / NULLIF(SUM(fraud_count), 0) AS recall,
  SUM(captured_fraud_amount) / NULLIF(SUM(fraud_amount), 0)
    AS fraud_amount_capture_rate
FROM workspace.fraud_detection.gold_daily_risk_kpis
GROUP BY 1
ORDER BY 1;

-- 4. Investigation queue by priority tier
SELECT
  priority_tier,
  COUNT(*) AS queue_count,
  AVG(score) AS average_score,
  MAX(score) AS maximum_score,
  SUM(amount_abs) AS queued_amount,
  SUM(CAST(is_above_selected_threshold AS BIGINT)) AS above_threshold_count
FROM workspace.fraud_detection.gold_investigation_queue
GROUP BY priority_tier
ORDER BY CASE priority_tier
  WHEN 'critical' THEN 1
  WHEN 'high' THEN 2
  WHEN 'medium' THEN 3
  ELSE 4
END;

-- 5. Top merchant categories in the investigation queue
SELECT
  COALESCE(mcc_category, 'unknown') AS mcc_category,
  COUNT(*) AS queue_count,
  AVG(score) AS average_score,
  SUM(amount_abs) AS queued_amount
FROM workspace.fraud_detection.gold_investigation_queue
GROUP BY 1
ORDER BY queue_count DESC
LIMIT 20;

-- 6. Channel and geography risk segments
SELECT
  COALESCE(transaction_type, 'unknown') AS transaction_type,
  COALESCE(merchant_location_category, 'unknown') AS merchant_location_category,
  COUNT(*) AS queue_count,
  AVG(score) AS average_score,
  SUM(amount_abs) AS queued_amount
FROM workspace.fraud_detection.gold_investigation_queue
GROUP BY 1, 2
ORDER BY queue_count DESC;

-- 7. Operational investigation queue (no outcome label)
SELECT
  investigation_rank,
  priority_tier,
  transaction_id,
  transaction_timestamp,
  score,
  amount_abs,
  is_above_selected_threshold,
  transaction_type,
  merchant_location_category,
  mcc_category,
  risk_reason_codes
FROM workspace.fraud_detection.gold_investigation_queue
ORDER BY investigation_rank
LIMIT 100;

-- 8. Retrospective queue audit; do not use this query for operational prioritization
SELECT
  priority_tier,
  COUNT(*) AS queue_count,
  SUM(CAST(actual_is_fraud AS BIGINT)) AS captured_fraud_count,
  SUM(CAST(actual_is_fraud AS BIGINT)) / NULLIF(COUNT(*), 0) AS audit_precision,
  SUM(CASE WHEN actual_is_fraud = 1 THEN amount_abs ELSE 0 END)
    AS captured_fraud_amount
FROM workspace.fraud_detection.gold_investigation_queue
GROUP BY priority_tier
ORDER BY CASE priority_tier
  WHEN 'critical' THEN 1
  WHEN 'high' THEN 2
  WHEN 'medium' THEN 3
  ELSE 4
END;
