# Databricks AI/BI Dashboard Guide

## Purpose

The dashboard presents final model evidence, risk trends, and an investigation queue from
the persisted Gold tables. It does not train models or alter pipeline data. Its source is
version controlled in `dashboards/fraud_risk_analytics.lvdash.json` and registered as a
Databricks Bundle resource in `resources/fraud_risk_dashboard.dashboard.yml`.

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

The numbered SQL statements are in [../sql/dashboard_queries.sql](../sql/dashboard_queries.sql)
for ad hoc inspection. The deployable dashboard contains equivalent portable SQL using bare
table names; Bundle parameters supply the catalog and schema.

## Dashboard Pages

1. **Executive Overview**: operating KPIs, monthly risk trends, queue mix, merchant risk,
   and champion-model evidence.
2. **Investigation Operations**: a top-100 ranked analyst queue without outcome labels.
3. **Retrospective Audit**: post-outcome queue precision and captured exposure proxy,
   intentionally separated from operational prioritization.

## Deployment

The default Bundle configuration uses SQL warehouse `7bd8a109f691d393` and namespace
`workspace.fraud_detection`. Override the Bundle variables if another environment is used.

```bash
databricks auth login \
  --host https://dbc-c80fffcf-363a.cloud.databricks.com \
  --profile fraud-risk-lakehouse

databricks bundle validate -t dev -p fraud-risk-lakehouse
databricks bundle deploy -t dev -p fraud-risk-lakehouse
databricks bundle summary -t dev -p fraud-risk-lakehouse
```

After deployment, open the dashboard URL from `bundle summary`, verify all datasets refresh,
and publish it from the dashboard UI. If UI edits are made later, export or generate the
updated dashboard definition before the next deployment so Git remains the source of truth.

## Interpretation Guardrails

- The champion model is selected using validation PR-AUC, not test performance.
- The operating threshold is selected on validation data using F1.
- Precision and recall shown by the dashboard are retrospective test metrics.
- `actual_is_fraud` is prohibited from operational queue ranking and display.
- Fraudulent transaction amount is an exposure proxy, not confirmed financial loss.
- No cost-based optimization is claimed because no explicit cost function exists.
