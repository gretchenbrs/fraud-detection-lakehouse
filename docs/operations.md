# Operations Guide

## Choose the operation

| Operation | When to use | Fits models? |
| --- | --- | --- |
| [Reporting refresh](https://dbc-c80fffcf-363a.cloud.databricks.com/jobs/1072479561746581) | Rebuild Gold and business views from existing evaluation tables | No |
| [Saved-model historical replay](https://dbc-c80fffcf-363a.cloud.databricks.com/jobs/349777192435211) | Demonstrate scoring with the frozen model in a fresh session | No |
| [Model release verification](https://dbc-c80fffcf-363a.cloud.databricks.com/jobs/799697975540454) | Create the reviewed release once, then verify its saved artifacts | Only if the release does not already exist |
| [Optional model tuning](https://dbc-c80fffcf-363a.cloud.databricks.com/jobs/495975921096138) | Run an intentional new candidate comparison | Yes |
| [Full rebuild](https://dbc-c80fffcf-363a.cloud.databricks.com/jobs/167675210256810) | Reproduce the historical experiment from the raw files | Yes, baseline models only |

The four-candidate tuning run retained the original logistic regression. See the
[measured results](model-selection-20260927.md). No additional search was started.
All tasks within a job run automatically. These static datasets have no refresh
feed, so no recurring schedule is configured. Define a real refresh cadence and
point-in-time input contract before enabling scheduled new-data scoring.

## Artifacts and outputs

- Raw inputs remain in the existing fraud_detection_raw volume.
- Saved model and preprocessing live in model_artifacts/lr_baseline_20260927.
- The release contains a frozen Delta copy of the test feature input.
- release.json records the chosen parameters, threshold, selection run and
  save/load score parity check. It is written only after verification succeeds.
- model_batch_scores contains replay scores and the immutable release identifier.
- experiment_model_* tables are reserved for future tuning runs.
- Existing model_* evaluation and gold_* tables retain the reviewed experiment
  until an explicit full rebuild or reporting refresh updates them.

The historical replay table is separate from the retrospective Gold audit.
Reporting refresh does not consume new streaming transactions or model_batch_scores.
There is no production inference endpoint or automatic champion promotion.

## Safe reruns

The saved-model release is immutable. Re-running its fit task reads the verified
release manifest and skips fitting. A failed partial artifact save deliberately
does not overwrite an existing directory; inspect it and use a new release ID for
a fresh attempt. Historical scoring overwrites only model_batch_scores.

Do not overlap full rebuild with reporting refresh. These jobs share model and
Gold tables, and per-job concurrency limits do not serialize different jobs.
The tuning job uses separate tables and never automatically promotes a candidate.
Before retraining or promoting a future release, retain the old model and metrics
and make selection using validation evidence, not repeated test comparisons.

## Code synchronization

GitHub stores reviewed source. Bundle deployment updates the files and job
definitions actually executed by Jobs. A Databricks editor Git folder is a separate
checkout; pulling it does not redeploy the Bundle. Use the deployed Job links above
when verifying a deployment. Do not edit deployed Bundle files as the permanent
source of truth; change source, test, commit, and deploy it again.

Local rollback archives are outside the Git repository under the Codex workspace's
backups directory. Aggregate evaluation evidence is saved under outputs. Neither
archive contains authentication tokens or downloaded individual transaction rows.
