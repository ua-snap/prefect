# NG-IDF pipeline flow

Runs the NG-IDF curation pipeline ([ua-snap/ng-idf_curation](https://github.com/ua-snap/ng-idf_curation)) on Chinook and documents every stage as Prefect artifacts.

NG-IDF means next-generation intensity-duration-frequency curves. The model and the method are H. Yan's NG-IDF app (PNNL). We run it for about 560 Alaska places × 12 climate datasets.

The science and the checks live in the curation repo (`ngidf/pipeline.py`, `ngidf/qa/`). This flow only drives that code over SSH, the same way the other flows in this repo do.

## What a run does

1. Clones or updates the curation repo on Chinook (`branch_name`, or an exact `curation_commit`).
2. Asks the pipeline for its plan. Each of the 25 stages has a **fingerprint**: a hash of its code, inputs and settings.
   - A stage whose fingerprint already has a passing result is **reused**, not run again.
   - A changed stage runs in a new directory. Nothing is overwritten.
3. Submits every stage that is ready. Independent stages, such as the 12 forcing datasets, run side by side on Slurm.
4. Waits for the jobs and runs the stage's checks (`finalize`), then repeats until nothing more can run.
5. Cuts a release when `release_name` is set and every stage passed.

## Artifacts

| Key | What it is |
|---|---|
| `ngidf-plan` | The model version (the author's commit and the SNAP mirror tag), the curation commit, and what will run or be reused |
| `ngidf-<stage>` | One per stage: <ul><li>the verdict</li><li>every check with its result</li><li>measurements</li><li>Slurm accounting</li><li>exactly what the fingerprint covers</li></ul> |
| `ngidf-<stage>-<table>` | Tables too large to inline, such as the forcing comparison with the author's files per cell |
| `ngidf-summary` | <ul><li>One verdict, with one row per stage</li><li>The warnings worth reading</li><li>Where the report is</li></ul> |

Reused stages get artifacts too, so every flow run is a complete record.

Status is always shown as an icon and a word: ✅ Passed, ⚠️ Passed with warnings, ❌ Failed.

The report for the dataset author is a separate self-contained HTML file written by the `qa` stage. The summary artifact gives its path.

## Parameters

| Parameter | Default | |
|---|---|---|
| `ssh_username`, `ssh_private_key_path` | | Chinook account that owns the data directory |
| `branch_name` | `main` | Branch of the **curation** repo (`ng-idf_curation`), not of this prefect repo |
| `working_directory` | `/import/beegfs/SNAP/jdpaul3` | Where the repo is cloned on Chinook |
| `model_version` | repo's `CURRENT` | Label of a file in `config/model_versions/` |
| `data_root` | `config/paths.env` | Override the data directory |
| `curation_commit` | branch head | Run an exact commit |
| `stop_before` | none | Stop before this stage and everything after it. Use `runs` to look at the reference runs before spending the ~6,700 production runs |
| `retry_failed` | `False` | Run failed stages again. The failed attempt is moved aside, not deleted |
| `release_name` | none | Cut a release when everything passed |
| `poll_seconds` | 120 | Slurm queue polling interval |
| `driver_python` | `$HOME/miniconda3/bin/python3` | Python 3.9 or later on Chinook. `pipeline.py` uses only the standard library; the stages build their own conda environments from lock files |

## A new model version

When the author updates the model (the soil-temperature DHSVM is expected by the end of 2026):

1. Mirror the commit into `ua-snap/NG_IDF` as tag `app-<sha7>`:
   ```bash
   git fetch https://github.com/hydro-yan/NG_IDF_AWS.git <commit>
   git tag -a app-<sha7> <commit> -m "..."
   git push https://github.com/ua-snap/NG_IDF.git refs/tags/app-<sha7>
   ```
2. Add `config/model_versions/<sha7>.env` to the curation repo, with `BASELINE_MODEL` set to the previous label. Commit it.
3. Run this flow with `model_version=<sha7>`, or update `CURRENT`.

The flow checks that the tag exists and resolves to the commit in the version file before it downloads anything.

Only the stages that read the model run again: app → DHSVM build → reference runs → production runs → coverage → QA. The forcing extraction (about 30 TB of reads) is reused.

The QA report then includes:
- a model-change section: what changed in the author's code, method settings and DHSVM source;
- a release comparison: how far the curves moved, by dataset, land cover and place.

## Requirements on Chinook

- The SSH account can read `ua-snap/NG_IDF`, which is private. The app stage clones it with `git`, so set up a credential helper or token for that account.
- Conda installed in `$HOME/miniconda3`, or set `driver_python`.
- Write access to the data directory in the curation repo's `config/paths.env`, or to `data_root`.

## Run

```bash
cd ng_idf
python ngidf_pipeline.py      # serves the deployment "ng-idf-pipeline"
```

## Tests

`test_ngidf_artifacts.py` covers the artifact text and the decision of what to run. It needs no server or SSH:

```bash
python -m pytest ng_idf/test_ngidf_artifacts.py
```
