# Databricks SQL Dashboard Guide

## Purpose

The dashboard presents final model evidence, risk trends, and an investigation queue from
the persisted Gold tables. It does not train models or alter pipeline data.

## Source Tables

- `workspace.fraud_detection.gold_model_scorecard`
- `workspace.fraud_detection.gold_daily_risk_kpis`
- `workspace.fraud_detection.gold_investigation_queue`

## Recommended Layout

| Section | Query | Visualization |
|---|---|---|
| Executive summary | 2 | KPI counters for precision, recall, alert rate, and amount capture |
| Model evidence | 1 | Scorecard table or grouped bars for PR-AUC and ROC-AUC |
| Risk trend | 3 | Multi-series line chart by month |
| Queue mix | 4 | Ordered bars by priority tier |
| Merchant risk | 5 | Horizontal bar chart for top MCC categories |
| Channel and geography | 6 | Heatmap or grouped bars |
| Investigation queue | 7 | Sortable detail table |
| Retrospective audit | 8 | Small audit table, clearly labeled as post-outcome analysis |

The numbered SQL statements are in [../sql/dashboard_queries.sql](../sql/dashboard_queries.sql).
Create one saved Databricks SQL query per numbered section, then add the recommended
visualization to a single dashboard canvas.

## Interpretation Guardrails

- The champion model is selected using validation PR-AUC, not test performance.
- The operating threshold is selected on validation data using F1.
- Precision and recall shown by the dashboard are retrospective test metrics.
- `actual_is_fraud` is prohibited from operational queue ranking and display.
- Fraudulent transaction amount is an exposure proxy, not confirmed financial loss.
- No cost-based optimization is claimed because no explicit cost function exists.
