# Model-family comparison and historical evaluation

## Locked protocol

This independent job compares logistic regression, Random Forest and Spark
gradient-boosted trees on the same V2 point-in-time features. It uses the materialized
2017/2018 feature tables from the preceding experiment, balanced class weights,
training-only preprocessing and a 30-day assumed label delay. It is a comparison
of three fixed configurations, not an exhaustive search of either tree family.

The primary selection metric is equal-weight mean recall at the top 1% of scored
transactions across 2017 and 2018 validation. Mean exact Spark PR-AUC breaks ties,
then model name makes any remaining tie deterministic. Test metrics never enter
selection. The protocol is development work on previously used validation data.

| Family | Fixed configuration |
| --- | --- |
| Logistic regression | 60 iterations, L2 regularization 0.05 |
| Random Forest | 80 trees, depth 8, 64 bins, subsample 0.8 |
| Gradient-boosted trees | 40 iterations, depth 5, 64 bins, step size 0.1, subsample 0.8 |

Every candidate saves its fitted preprocessing and model under a unique run path
in the existing model_artifacts Volume. A 100-row save/load replay check precedes
validation scoring. All new table names begin with experiment_v2_family_.

## Final historical test

The freeze task records the selected family, model path, source Delta version,
selection rule and validation-derived score cutoff before the test task starts.
The cutoff is the lowest score in the top 1% of 2018 validation transactions.
Ties can cause a fixed score cutoff to flag more than 1%. The separate top-1%
ranking metric resolves ties by transaction ID and enforces the review count.

The 2019 evaluation loads the saved 2018-validation model without refitting.
Consequently model fitting ends 30 days before 2017-12-31. MCC encodings remain
frozen at that forecast origin. Behavioral windows can use earlier 2018/2019
transactions as they would become available, but never future or same-time peers.
The same pinned Silver snapshot supplies the historical test features.

2019 has previously been inspected for older models. Report it as a historical
test comparison, not a new untouched holdout. No test-based threshold adjustment
or automatic production promotion occurs. Annual top-1% capture is an offline
ranking measure, not evidence of satisfying a daily staffing limit or real-time
latency requirement. Labels and snapshots have the limitations documented in
[V2 experiments](v2-experiments.md).

## Execution

Run the independent bundle job fraud_model_comparison after V2 features exist.
The job executes six validations, freezes the selection, then scores the historical
test. The original batch workflow, Gold outputs and released model are separate.

Initial run: 913689895943946 (results pending at job submission).
