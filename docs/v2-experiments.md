# V2 Point-in-Time Experiments

This is an isolated experiment, not a replacement for the reviewed model release.
The original model, scores, dashboard and Gold tables remain unchanged.

## Question and scope

Does a small set of past transaction history features improve validation ranking
and review-budget capture, without relying on current customer/card snapshots or
self-inclusive target encoding? Are full inverse-frequency class weights helpful?

The existing result is approximately 0.979 ROC-AUC and 0.062 PR-AUC. These measure
different aspects of ranking and must not be compared as if they were the same
metric. A recalled older "98% AUC" may be ROC-AUC, but its code is needed to verify.

## Time boundaries

- Fold 2017: training through 2016, validation during 2017.
- Fold 2018: training through 2017, validation during 2018.
- The experiment does not read transaction rows dated 2019 or later.
- A 30-day fraud-label delay is an explicit assumption, not observed metadata.
- Fitting excludes the final 30 days of labels before each forecast origin.
- Training MCC features use only strictly older, matured labels; validation uses
  the mapping frozen at the forecast origin. A fixed 0.001 cold-start prior is
  used when no historical labels are available.

Silver's Delta version is pinned before materializing features. Behavioral windows
include all earlier transactions, including unlabeled ones. Filtering to supervised
rows happens after history construction. Same-second peers are excluded because
the source does not establish their event order. The source timestamps have minute
precision; these features are not a solution to sub-second arrival ordering.

## Candidate comparisons

| Candidate | Feature set | Weight power |
| --- | --- | ---: |
| static_moderate | Transaction amount/time/category only | 0.5 |
| history_moderate | Static plus past card/user/merchant behavior | 0.5 |
| history_mcc_moderate | History plus delayed historical MCC encoding | 0.5 |
| history_mcc_unweighted | Same history and MCC | 0.0 |
| history_mcc_balanced | Same history and MCC | 1.0 |
| snapshot_ablation | History/MCC plus current credit/income/debt snapshots | 0.5 |

Weight power applies to the training negative/positive ratio. Weights are normalized
to mean one, so comparisons do not accidentally change regularization strength
through a changing overall weight scale. Validation is never balanced or sampled.
Snapshot features are retained only in an explicitly marked diagnostic candidate.
Transaction-error fields are omitted because availability depends on the intended
decision time (before authorization versus after a failed attempt).

History includes card counts and amount totals over 1h/24h, user amount mean and
standard deviation over 30d, time since the prior transaction, amount deviation,
and first observed user/merchant interaction. All candidates use the same
regularized logistic regression (60 iterations, regParam=0.05). Keeping this fixed
isolates feature and weighting effects before any broader model-family search.

## Evaluation and artifacts

PR-AUC is Spark areaUnderPR with numBins=0, disabling curve downsampling. This is
not scikit-learn Average Precision. The earlier baseline used Spark's default
1000 bins; tiny differences from that historical value are not evidence of better
features. Compare the new candidates against each other on identical folds.

Each candidate reports PR-AUC, ROC-AUC, precision/recall at the top 0.1%, 0.5%, and
1%, and validation-selected thresholds. Threshold candidates combine the old grid
with validation score quantiles. Moving a threshold does not change PR-AUC.

All outputs use the experiment_v2_* prefix. Metrics are checkpointed after every
candidate; score tables allow metric inspection without fitting again. The final
leaderboard reports mean and worst-year PR-AUC plus top-1% capture. Nothing is
automatically promoted or scored on the already-inspected 2019 test set.

The separate fraud_feature_experiments Job runs preparation and Spark guardrail
tests, followed by the two validation folds and the leaderboard. No schedule is
enabled. Use this job separately from full rebuild to avoid shared compute pressure.

## Checks and limitations

Cloud fixtures verify unlabeled-history inclusion, same-time exclusion, future-row
invariance, self-label exclusion, delayed label use, and validation-label isolation.
Local tests verify chronological fold boundaries and normalized class weights.

The data does not contain actual label-availability timestamps or versioned customer
attributes. Removing snapshot variables improves temporal defensibility but cannot
establish a fully production-valid backtest. The original labeled population may
also be a selected subset. Confirm data provenance and label coverage before making
claims about future deployment performance.

## Completed experiment: 2026-09-27

Run 449128605531516 succeeded in 17m21s against Silver version 9. All cloud
chronology fixtures passed. The following are validation results, not test results.

| Candidate | 2017 PR-AUC | 2018 PR-AUC | 2018 recall at top 1% |
| --- | ---: | ---: | ---: |
| static_moderate | 0.005890 | 0.030863 | 18.91% |
| history_moderate | 0.008246 | 0.064576 | 38.55% |
| history_mcc_moderate | 0.009531 | 0.065399 | 37.81% |
| history_mcc_unweighted | 0.004746 | 0.021365 | 16.39% |
| history_mcc_balanced | 0.014131 | 0.099745 | 45.92% |
| snapshot_ablation | 0.010013 | 0.070139 | 38.43% |

The history/MCC balanced candidate ranks first in both folds. Its 2018 ROC-AUC
is 0.960016; precision among the top 1% is 8.00%. At validation-selected threshold
0.9, precision is 16.66%, recall 37.26%, and F1 0.23023. Threshold-selected results
are development estimates, not unbiased estimates of future performance.

2017 contains 172 fraud labels among 937,284 rows; 2018 contains 1,629 among
934,599. Different prevalence makes absolute PR-AUC across years misleading.
Use within-year comparisons and review-budget capture, not only the mean leaderboard.

History improves the moderate-weight baseline in both years. Full class weighting
also helps on the tested history/MCC representation; this does not establish that
full weighting is best for every feature set or model family. Snapshot fields are
not required for the best observed result and remain excluded from the candidate.

Legacy audit run 567513022437606 succeeded. On the same 934,599 validation rows
(1,629 fraud labels), recomputing the old baseline with numBins=0 gives PR-AUC
0.069505 rather than the earlier downsampled 0.063958, ROC-AUC 0.978160, and
top-1% recall 37.75% (precision 6.58%). The new candidate improves exact PR-AUC
by about 43.5% and top-1% recall to 45.92%, but reduces ROC-AUC to 0.960016.
This is a ranking tradeoff, not an across-the-board improvement. Feature sets,
label-delay assumptions and training windows differ; only within-experiment
ablations isolate individual changes.

No model has been promoted. Next gates are review of feature availability
assumptions and a frozen candidate/threshold before
any final evaluation. The previously inspected 2019 test period is not a fresh
holdout; further evaluation there must be disclosed as such.
