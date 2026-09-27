# Reviewed Model Selection

Source: completed Databricks run 473360204017919 (44 minutes 6 seconds).
Tuning took 30 minutes 43 seconds. Metrics were read from persisted tables.

| Candidate | Validation PR-AUC | Validation ROC-AUC | Selected threshold |
| --- | ---: | ---: | ---: |
| logistic_regression_baseline | 0.0639577372 | 0.9781392201 | 0.98 |
| logistic_regression_elastic_net | 0.0561393524 | 0.9773801588 | 0.95 |
| random_forest_deeper | 0.0266633456 | 0.9680019352 | 0.50 |
| random_forest_baseline | 0.0227713377 | 0.9628862747 | 0.70 |

Keep the original logistic regression: max_iter=40, reg_param=0.05,
elastic_net_param=0.0, training-only inverse-frequency class weights.
The additional candidates did not beat the baseline. No second search is justified
by these results alone. The threshold maximizes validation F1; it is not a
cost-optimal business decision.

The selected model's test PR-AUC is 0.0617798751 and ROC-AUC is 0.9789456723.
There are 777,339 test transactions and 1,360 fraud labels. Validation contains
934,599 transactions and 1,629 fraud labels. Test metrics did not select the winner.

## Limits

This is a retrospective portfolio experiment, not production certification.
Snapshot customer/card attributes are not guaranteed to represent what was known
at transaction time. Training MCC encoding uses training labels, including each
training row's own label; out-of-fold or past-only encoding would be preferable
for a future modeling study. Test results have already been viewed across project
iterations and must not guide further tuning.

## Saved-Model Verification

Databricks run 1063843209650032 succeeded in 4 minutes 54 seconds.
The reviewed logistic regression and all fitted preprocessing were saved to the
dedicated model_artifacts volume. A 1,000-row save/load parity check had maximum
absolute score difference 0.0. A separate task loaded the model and frozen replay
features and scored 777,339 rows with 777,339 distinct transaction IDs, zero invalid
scores, and 1,406 flags at the frozen 0.98 threshold. This check does not select or
retune any model parameter.

Reporting-only run 399064373496488 succeeded in 86 seconds without training.
The local suite passed 62 tests. The optional isolated tuning job was deployed but
not re-run, because repeating the same four-candidate search has no benefit.

Persisted historical replay does not ingest new transactions. A new-data scoring
path needs the frozen training MCC mapping, a defined input contract, and
point-in-time feature availability before an automatic schedule is appropriate.

## Operations

The full rebuild remains available on demand and retrains baseline models.
Tuning is a separate job. Reporting refresh only reads persisted model outputs.
The saved-model fit/verify job creates an immutable model release with its fitted
preprocessing, verifies save/load score parity, and runs scoring in a fresh task.
The scoring-only job reuses that release without fitting.
All tasks inside each job run automatically in dependency order. No timer is
enabled for static input files. Do not run rebuild, tuning, or reporting refresh
concurrently because the existing evaluation tables are shared.
