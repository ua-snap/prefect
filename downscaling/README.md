# CMIP6 Statistical Downscaling Pipeline

Comprehensive workflow for statistically downscaling daily CMIP6 climate model data to high-resolution Arctic grids (4km or 12km) using Quantile Delta Mapping (QDM) bias adjustment with ERA5 as reference data. This flow orchestrates tasks across multiple codebases: it clones and executes processing scripts from the [`cmip6-utils`](https://github.com/ua-snap/cmip6-utils) repository on Chinook, and uses the regridding flow (`~/prefect/regridding/regrid_cmip6.py`) and supporting modules (`~/prefect/pipelines/`, `~/prefect/bias_adjust/`, `~/prefect/utils/`) from this same Prefect repository.

---

## Table of Contents

1. [Overview](#overview)
1. [Prerequisites](#prerequisites)
1. [Flow Parameters](#flow-parameters)
1. [Running the Flow](#running-the-flow)
1. [Output Directory Structure](#output-directory-structure)
1. [Common Workflows](#common-workflows)
1. [Troubleshooting](#troubleshooting)
1. [Variable, Model, and Scenario Support](#variable-model-and-scenario-support)

---

## Overview

This Prefect flow orchestrates the complete statistical downscaling pipeline:

1. **Cascade Regridding**: Three-step regridding from native CMIP6 grids to high-resolution Arctic grids
2. **Data Conversion**: Convert NetCDF to Zarr format for efficient parallel processing
3. **Bias Adjustment Training**: Train QDM models using historical CMIP6 vs. ERA5 data
4. **Bias Adjustment Application**: Apply trained adjustments to historical and future scenarios
5. **Derived Variables**: Compute tasmin from adjusted tasmax - dtr

The pipeline handles land/sea masking at each regridding stage, calendar conversions, jitter preprocessing for zero-inflated variables, and frequency adaptation.

---

## Prerequisites

### 1. Curated ERA5 Data

Before running this flow, you must curate daily ERA5 files using the [`wrf-downscaled-era5-curation`](https://github.com/ua-snap/wrf-downscaled-era5-curation) repository. This produces daily NetCDF files on the target Arctic grid (4km or 12km).

**Important**: For development/testing, curate ERA5 data to **your own directory** (e.g., `/beegfs/CMIP6/your-username/era5_4km/`). Do not use the shared production directory (`/beegfs/CMIP6/arctic-cmip6/era5/`) to avoid corrupting the reference dataset.

### 2. ERA5 Variable Preparation

Some ERA5 variables require additional preprocessing using `~/cmip6-utils/downscaling/prep_era5_variables.py`:

- **Temperature variables** (`t2_min`, `t2_max`): Standardize variable and file names, convert from Celsius to Kelvin
- **Precipitation** (`rainnc_sum`): Standardize variable and file names

Other variables (e.g., `rh2_mean`, `rh2_min`, `snow_sum`, `wspd10_mean`) can be used directly from curation without prep.

**Example**:
```bash
# On Chinook
conda activate cmip6-utils
python ~/cmip6-utils/downscaling/prep_era5_variables.py \
    --input_dir /path/to/your/curated/era5 \
    --output_dir /path/to/your/era5/for/downscaling \
    --old t2_min t2_max rainnc_sum \
    --new t2min t2max pr \
    --celsius-to-kelvin t2_min t2_max
```

After prep, your ERA5 data for downscaling should be organized like:
```
/path/to/your/era5/for/downscaling
├── t2max/
│   ├── t2max_1980_era5_4km_3338.nc
│   ├── t2max_1981_era5_4km_3338.nc
│   └── ...
├── pr/
├── rh2_mean/
└── ...
```

### 3. Raw CMIP6 Data Access

You need access to the raw daily CMIP6 data on Chinook at:
```
/beegfs/CMIP6/arctic-cmip6/CMIP6/
```

This is a shared read-only resource.

### 4. Python Environment

Ensure you have:
- Prefect installed in your Python environment
- SSH access to Chinook with key-based authentication
- The `cmip6-utils` conda environment available on Chinook

---


## Flow Parameters

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `ssh_username` | str | Chinook username | `"your-username"` |
| `ssh_private_key_path` | str | Path to SSH private key | `"/home/your-username/.ssh/id_rsa"` |
| `repo_name` | str | Repository name | `"cmip6-utils"` |
| `branch_name` | str | Git branch to use | `"main"` (or dev branch) |
| `conda_env_name` | str | Conda environment name | `"cmip6-utils"` |
| `cmip6_dir` | str | Path to raw CMIP6 data | `"/beegfs/CMIP6/arctic-cmip6/CMIP6"` |
| `reference_dir` | str | Path to curated ERA5 data | `"/path/to/your/curated/era5"` |
| `project_base_dir` | str | Base directory where repo is cloned | `"/beegfs/CMIP6/your-username/downscaling"` |
| `run_name` | str | Subdirectory name for this specific run | `"test_12km_tasmin_tasmax_dtr_pr"` |
| `variables` | str | Space-separated CMIP6 variable IDs | `"tasmin tasmax dtr pr"` |
| `models` | str | Space-separated model names or `"all"` | `"CESM2 GFDL-ESM4"` or `"all"` |
| `scenarios` | str | Space-separated scenarios or `"all"` | `"historical ssp370"` or `"all"` |
| `partition` | str | SLURM partition | `"t2small"` |
| `cascade_grid_coords_file` | str | Template file for intermediate grids | See below |
| `final_grid_template_file` | str | ERA5 file to use as final grid template | See below |
| `flow_steps` | str | Which steps to run (see [Common Workflows](#common-workflows)) | `"all"` |
| `first_regrid_linspace_step` | float | Step size for first intermediate grid | `0.5` |
| `second_regrid_linspace_step` | float | Step size for second intermediate grid | `0.25` |
| `resolution` | int | Target resolution in km | `4` or `12` |

### Template File Parameters

**`cascade_grid_coords_file`** - CMIP6 file used to extract grid coordinates for creating intermediate grids. Any CMIP6 file with a well-behaved grid works.
- **Default (recommended)**: `/beegfs/CMIP6/arctic-cmip6/CMIP6/ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/day/tas/gn/v20200528/tas_day_CESM2_ssp370_r11i1p1f1_gn_20150101-20241231.nc`

**`final_grid_template_file`** - ERA5 file used to create the final target grid. Any ERA5 variable file at your target resolution works:
- **4km (recommended)**: `/beegfs/CMIP6/arctic-cmip6/era5/daily_era5_4km_3338/t2/t2_1965_era5_4km_3338.nc`
- **12km (recommended)**: `/beegfs/CMIP6/arctic-cmip6/era5/daily_era5_12km_3338/t2/t2_1965_era5_12km_3338.nc`

---

## Running the Flow

### Create a python environment
```bash
micromamba create -n prefect python=3.11 pip -c conda-forge
micromamba activate prefect

pip install prefect==2.14.2
pip install "griffe<1.0"
pip install “pydantic<2.11”
pip install paramiko

python -m pip install .
```

### Start the flow on the shared prefect server in a screen session

```bash
screen -S prefect
micromamba activate prefect
prefect config set PREFECT_API_URL=https://prefect.earthmaps.io/api
cd ~/prefect # make sure you are on the appropriate git branch
export PYTHONPATH=$PWD
python downscaling/downscale_cmip6.py
```

### Example: Development Run (12km, 2 variables, 2 models)

From the Prefect server UI, start a new flow run using parameters like these:

```python
    parameters={
        "ssh_username": "jdpaul3", # your username
        "ssh_private_key_path": "/home/jdpaul3/.ssh/id_rsa", # your key
        "repo_name": "cmip6-utils",
        "branch_name": "main", # or dev branch
        "conda_env_name": "cmip6-utils",
        "cmip6_dir": "/beegfs/CMIP6/arctic-cmip6/CMIP6",
        "reference_dir": "/beegfs/CMIP6/jdpaul3/era5_12km/for_downscaling", # your curated & prepped ERA5
        "project_base_dir": "/beegfs/CMIP6/jdpaul3/downscaling", # your output dir
        "run_name": "test_12km_tasmax_pr",
        "variables": "tasmax pr",
        "models": "CESM2 GFDL-ESM4",
        "scenarios": "historical ssp370",
        "partition": "t2small",
        "cascade_grid_coords_file": "/beegfs/CMIP6/arctic-cmip6/CMIP6/ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/day/tas/gn/v20200528/tas_day_CESM2_ssp370_r11i1p1f1_gn_20150101-20241231.nc",
        "final_grid_template_file": "/beegfs/CMIP6/arctic-cmip6/era5/daily_era5_12km_3338/t2/t2_1965_era5_12km_3338.nc",
        "flow_steps": "all",
        "first_regrid_linspace_step": 0.5,
        "second_regrid_linspace_step": 0.25,
        "resolution": 12,
    }
```

### Expected Runtime

- **Minimum**: ~6 hours (12km resolution)
- **Typical**: ~10 hours (4km resolution)
- **Factors**: Node availability, data resolution, number of variables/models/scenarios, file system performance

Runtime varies significantly. Check SLURM job progress via `squeue -u your-username` on Chinook, or simply monitor the flow run in the Prefect UI.

---

## Output Directory Structure

All outputs are written to `{project_base_dir}/{run_name}/`:

```
{project_base_dir}/{run_name}/
├── slurm/                          # SLURM batch scripts and logs
│   ├── first_regrid/
│   ├── second_regrid/
│   ├── final_regrid/
│   ├── train/
│   └── adjust/
├── first_regrid/                   # Intermediate regrid step 1
│   └── {model}/{scenario}/day/{variable}/
├── second_regrid/                  # Intermediate regrid step 2
│   └── {model}/{scenario}/day/{variable}/
├── final_regrid/                   # Final regridded NetCDF (ERA5 grid)
│   └── {model}/{scenario}/day/{variable}/
├── cmip6_zarr/                     # CMIP6 data in Zarr format
│   └── {variable}_{model}_{scenario}.zarr/
├── era5_zarr/                      # ERA5 reference data in Zarr format
│   └── {variable}_era5.zarr/
├── trained_datasets/               # QDM training weights (per model/variable)
│   └── {variable}_{model}_historical_quantiles.zarr/
├── adjusted/                       # ⭐ FINAL DOWNSCALED OUTPUT ⭐
│   └── {variable}_{model}_{scenario}_adjusted.zarr/
├── cmip6_dtr/                      # Derived DTR from raw CMIP6 (if requested)
├── era5_dtr/                       # Derived DTR from ERA5 (if requested)
├── first_regrid_target_file.nc     # Grid definition files
├── second_regrid_target_file.nc
└── final_regrid_target_file.nc
```

### Key Output Files

- **Primary output**: `adjusted/{variable}_{model}_{scenario}_adjusted.zarr/` - Bias-adjusted downscaled data at target resolution
- **Intermediate products**: Regridded NetCDF and Zarr files useful for QC
- **Training weights**: `trained_datasets/` - Reusable across scenarios for the same model/variable

---

## Common Workflows

### Full Pipeline (Recommended for First Run)

```python
"flow_steps": "all"
```

Runs all 18 steps sequentially. Use this for initial runs or when you want complete outputs.

### Partial Runs (Resume from Failure or Re-run Specific Steps)

The `flow_steps` parameter accepts space-separated step names:

**Available steps** (in execution order):
1. `create_remote_directories`
2. `clone_and_install_repo`
3. `generate_batch_files`
4. `process_dtr` (if dtr or tasmin requested)
5. `create_first_regrid_target_file`
6. `first_cmip6_regrid`
7. `create_second_regrid_target_file`
8. `second_cmip6_regrid`
9. `create_final_regrid_target_file`
10. `final_cmip6_regrid`
11. `process_era5_dtr` (if dtr or tasmin requested)
12. `ensure_reference_data_in_scratch`
13. `convert_era5_to_zarr`
14. `convert_cmip6_to_zarr`
15. `train_bias_adjustment`
16. `bias_adjustment`
17. `derive_cmip6_tasmin` (if tasmin requested)

**Example: Re-run only bias adjustment** (after fixing training data):
```python
"flow_steps": "bias_adjustment"
```

**Example: Resume from second regrid onward** (after first regrid completed):
```python
"flow_steps": "second_cmip6_regrid final_cmip6_regrid convert_era5_to_zarr convert_cmip6_to_zarr train_bias_adjustment bias_adjustment"
```

**Use case**: Intermittent file system errors may cause specific steps to fail. Identify the failed step from logs and re-run from that point forward.

### Variable Grouping Recommendations

Run variables in these recommended groups for efficiency and logical dependencies:

1. **Temperature & Precipitation Group**:
   ```python
   "variables": "tasmin tasmax dtr pr"
   ```
   - Includes all temperature-related variables
   - DTR enables tasmin derivation from tasmax - dtr
   - Precipitation is independent but commonly grouped with temperature

2. **Wind, Snow, and Humidity Group**:
   ```python
   "variables": "sfcWind snw hurs hursmin"
   ```
   - Surface wind speed
   - Snow amount (land variable with masking)
   - Relative humidity variables

**Note**: You can run any variable(s) individually, but these groups follow common use patterns and ensure derived variables work correctly.

---

## Troubleshooting

### Where to Look for Errors

All SLURM job outputs are written to:
```
{project_base_dir}/{run_name}/slurm/{step_name}/{job_name}_{job_id}_{array_id}.out
```

**Key log locations**:
- **Regridding errors**: `slurm/first_regrid/`, `slurm/second_regrid/`, `slurm/final_regrid/`
- **Training errors**: `slurm/train/train_qm_{model}_{variable}_{job_id}.out`
- **Bias adjustment errors**: `slurm/adjust/bias_adjust_{model}_{scenario}_{variable}_{job_id}.out`

### Common Issues

#### 1. File System Lag
**Symptom**: Jobs fail with "File not found" despite files existing
**Cause**: BeeGFS metadata caching; files created by one node may not be visible to another immediately
**Solution**:
- Re-run the failed step after a few minutes
- Flow includes automatic cache refresh diagnostics (logged as "FORCING FILESYSTEM CACHE REFRESH")

#### 2. Land/Sea Masking Errors
**Symptom**: `AssertionError` or masking-related errors during regridding
**Cause**: Missing source land mask (`sftlf`) for a model, or incorrect variable type classification
**Solution**:
- Check `cmip6-utils/regridding/config.py` for `model_sftlf_lu` (defines model-specific land mask paths)
- Verify variable is correctly classified in `landsea_variables` (only land-only variables like `snw` should be listed)
- Note: Land masks are regridded and applied at **each of the three regridding stages** to maintain accurate coastline representation

#### 3. Missing Variables for Specific Models
**Symptom**: Training or bias adjustment fails for certain model/variable combinations
**Cause**: Not all CMIP6 models provide all variables
**Solution**: Check the [Variable/Model/Scenario Support](#variable-model-and-scenario-support) table (TBD) or examine raw CMIP6 data availability

#### 4. Zero-Inflated Variable Warnings
**Symptom**: Warnings about jitter or frequency adaptation during training
**Cause**: Expected behavior for precipitation, snow, DTR (variables with many zero values)
**Solution**: These are informational; check that `jitter_under_lu` and `adapt_freq_thresh_lu` in `cmip6-utils/bias_adjust/luts.py` include your variable

#### 5. Calendar Conversion Errors
**Symptom**: Time coordinate mismatches between CMIP6 and ERA5
**Cause**: CMIP6 uses various calendars (noleap, 360_day, etc.); ERA5 uses standard Gregorian
**Solution**: Flow automatically converts calendars; check logs for "Converting reference calendar to noleap"

### Debugging Tips

1. **Check SLURM job status**: `squeue -u your-username` on Chinook
2. **Review most recent logs**: `tail -100 slurm/{step}/{latest_file}.out`
3. **Search for errors**: `grep -i error slurm/{step}/*.out`
4. **Verify intermediate outputs exist**: Check that regridded files were created before Zarr conversion
5. **Test with minimal parameters**: Start with 1 model, 1 scenario, 1 variable for debugging

---

## Variable, Model, and Scenario Support

### Recommended Variable Combinations

| Group | Variables | Notes |
|-------|-----------|-------|
| Temperature & Precipitation | `tasmin tasmax dtr pr` | DTR enables tasmin derivation |
| Snow | `snw` | snw requires conservative interpolation, cannot be mixed with other variables or interpolation methods might be mixed in regridding batch files |
| Wind, Humidity | `sfcWind hurs hursmin` | 

### Supported Variables

| CMIP6 Variable | ERA5 Equivalent | Operator | Interpolation | Notes |
|----------------|-----------------|----------|---------------|-------|
| `tasmax` | `t2max` | + (additive) | Bilinear | Daily maximum temperature |
| `tasmin` | `t2min` | + (additive) | Bilinear | Derived from tasmax - dtr |
| `tas` | `t2` | + (additive) | Bilinear | Daily mean temperature |
| `dtr` | `dtr` | * (multiplicative) | Bilinear | Diurnal temperature range |
| `pr` | `pr` | * (multiplicative) | Bilinear | Precipitation, zero-inflated |
| `hurs` | `rh2_mean` | + (additive) | Bilinear | Near-surface relative humidity |
| `hursmin` | `rh2_min` | + (additive) | Bilinear | Daily minimum relative humidity |
| `snw` | `snow_sum` | * (multiplicative) | Conservative | Surface snow amount, land-only, zero-inflated |
| `sfcWind` | `wspd10_mean` | * (multiplicative) | Bilinear | Near-surface wind speed |

### Model and Scenario Availability

The following tables summarize source data availability by model, scenario, and variable. To confirm availability for your specific combination, examine the data on Chinook using this filepath template:
`/beegfs/CMIP6/arctic-cmip6/CMIP6/{experiment}/{institution}/{model}/{scenario}/{variant}/day/{variable}/
`

#### `pr` — Precipitation

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | ✓ | ✓ | — | — | ✓ | |
| CNRM-CM6-1-HR | ✓ | ✓ | — | — | ✓ | |
| E3SM-2-0 | ✓ | — | — | ✓ | — | |
| EC-Earth3-Veg | ✓ | ✓ | — | ✓ | ✓ | |
| GFDL-ESM4 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| HadGEM3-GC31-LL | ✓ | ✓ | ✓ | — | ✓ | |
| HadGEM3-GC31-MM | ✓ | ✓ | — | — | ✓ | |
| KACE-1-0-G | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| NorESM2-MM | ✓ | ✓ | ✓ | ✓ | ✓ | |
| TaiESM1 | ✓ | ✓ | ✓ | ✓ | ✓ | |

#### `tasmax` — Daily Maximum Near-Surface Air Temperature

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | — | — | — | — | — | No data available. |
| CNRM-CM6-1-HR | ✓ | ✓ | — | — | ✓ | |
| E3SM-2-0 | ✓ | — | — | ✓ | — | |
| EC-Earth3-Veg | ✓ | — | — | ✓ | ✓ | |
| GFDL-ESM4 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| HadGEM3-GC31-LL | ✓ | ✓ | ✓ | — | ✓ | |
| HadGEM3-GC31-MM | ✓ | ✓ | — | — | ✓ | |
| KACE-1-0-G | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| NorESM2-MM | ✓ | ✓ | ✓ | ✓ | ✓ | |
| TaiESM1 | ✓ | ✓ | ✓ | ✓ | ✓ | |

#### `tasmin` — Daily Minimum Near-Surface Air Temperature

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | — | — | — | — | — | No data available. |
| CNRM-CM6-1-HR | ✓ | ✓ | — | — | ✓ | |
| E3SM-2-0 | ✓ | — | — | ✓ | — | |
| EC-Earth3-Veg | ✓ | — | — | ✓ | ✓ | |
| GFDL-ESM4 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| HadGEM3-GC31-LL | ✓ | ✓ | ✓ | — | ✓ | |
| HadGEM3-GC31-MM | ✓ | ✓ | — | — | ✓ | |
| KACE-1-0-G | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| NorESM2-MM | ✓ | ✓ | ✓ | ✓ | ✓ | |
| TaiESM1 | ✓ | ✓ | ✓ | ✓ | ✓ | |

#### `snw` — Surface Snow Amount

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| CNRM-CM6-1-HR | ✓ | — | — | — | — | |
| E3SM-2-0 | — | — | — | — | — | No data available. |
| EC-Earth3-Veg | ✓ | — | — | — | — | |
| GFDL-ESM4 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| HadGEM3-GC31-LL | ✓ | — | ✓ | — | — | |
| HadGEM3-GC31-MM | ✓ | — | — | — | — | |
| KACE-1-0-G | — | — | — | — | — | No data available. |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | ssp126 and ssp585 data extends beyond 2100. |
| NorESM2-MM | — | — | — | — | — | No data available. |
| TaiESM1 | — | — | — | — | — | No data available. |

#### `hurs` — Near-Surface Relative Humidity

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| CNRM-CM6-1-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| E3SM-2-0 | — | — | — | — | — | No data available. |
| EC-Earth3-Veg | ✓ | ✓ | ✓* | ✓ | ✓ | *ssp245 only covers 2061–2100. |
| GFDL-ESM4 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| HadGEM3-GC31-LL | ✓ | ✓ | ✓ | — | ✓ | |
| HadGEM3-GC31-MM | ✓ | ✓ | — | — | ✓ | |
| KACE-1-0-G | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | ssp126 and ssp585 data extends beyond 2100. |
| NorESM2-MM | ✓ | ✓ | ✓ | ✓ | ✓ | |
| TaiESM1 | ✓ | ✓ | ✓ | ✓ | ✓ | |

#### `hursmin` — Daily Minimum Near-Surface Relative Humidity

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | — | — | — | — | — | No data available. |
| CNRM-CM6-1-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| E3SM-2-0 | — | — | — | — | — | No data available. |
| EC-Earth3-Veg | ✓ | ✓ | ✓ | ✓ | ✓ | |
| GFDL-ESM4 | — | — | — | — | — | No data available. |
| HadGEM3-GC31-LL | — | — | — | — | — | No data available. |
| HadGEM3-GC31-MM | — | — | — | — | — | No data available. |
| KACE-1-0-G | ✓ | — | — | — | ✓ | |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | ssp126 and ssp585 data extends beyond 2100. |
| NorESM2-MM | — | — | — | — | — | No data available. |
| TaiESM1 | — | — | — | — | — | No data available. |

#### `sfcWind` — Near-Surface Wind Speed

| Model | historical | ssp126 | ssp245 | ssp370 | ssp585 | Notes |
|-------|:----------:|:------:|:------:|:------:|:------:|-------|
| CESM2 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| CNRM-CM6-1-HR | ✓ | — | — | — | — | |
| E3SM-2-0 | — | — | — | — | — | No data available. |
| EC-Earth3-Veg | ✓ | — | — | — | — | |
| GFDL-ESM4 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| HadGEM3-GC31-LL | ✓ | ✓ | ✓ | — | ✓ | |
| HadGEM3-GC31-MM | ✓ | ✓ | — | — | ✓ | |
| KACE-1-0-G | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MIROC6 | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MPI-ESM1-2-HR | ✓ | ✓ | ✓ | ✓ | ✓ | |
| MRI-ESM2-0 | ✓ | ✓ | ✓ | ✓ | ✓ | ssp126 and ssp585 data extends beyond 2100. |
| NorESM2-MM | ✓ | ✓ | ✓ | ✓ | ✓ | |
| TaiESM1 | ✓ | ✓ | ✓ | ✓ | ✓ | |



---

## Additional Notes

### QDM Bias Adjustment Details

- **Additive adjustment** (`+`): Used for temperature and humidity (absolute values matter)
- **Multiplicative adjustment** (`*`): Used for precipitation, snow, wind (relative changes matter)
- **Jitter preprocessing**: Applied to zero-inflated variables (pr, dtr, snw) via `jitter_under_thresh` to add tiny random noise to exact zeros, preventing quantile mapping mathematical errors
- **Frequency adaptation**: Applied to pr and snw via `adapt_freq_thresh` to correct **frequency bias**, where climate models simulate too many or too few event days (wet days, snow days) compared to ERA5. The threshold (e.g., `0.254 mm d-1` for pr, `0.254 kg m-2` for snw) separates "event days" from "non-event days," and the QDM algorithm adjusts both the frequency of these events and their magnitude to match ERA5 climatology. These thresholds match the "trace" precipitation threshold used by NWS.

See `cmip6-utils/bias_adjust/luts.py` for variable-specific configurations.

### Cascade Regridding Strategy

Three-step regridding minimizes interpolation errors for large grid spacing differences:
1. **First regrid**: CMIP6 native → intermediate grid 1 (step=0.5)
2. **Second regrid**: Intermediate grid 1 → intermediate grid 2 (step=0.25)
3. **Final regrid**: Intermediate grid 2 → ERA5 target grid

**Interpolation methods**:
- **Conservative** (`remapcon`): Used for `snw` (surface snow amount) to preserve mass/area integrals
- **Bilinear** (`remapbil`): Used for all other variables (temperature, precipitation, humidity, wind)

**Land/sea masking**: Applied independently at **each regridding stage** using model-specific land masks (`sftlf`) that are regridded to match each intermediate grid. This ensures ocean grid cells are masked (set to NaN) at every stage, preventing interpolation artifacts at coastlines. The masking process:
1. For each regridding stage, create a model-specific `sftlf` target file at that stage's resolution
2. Regrid both the climate variable and the land mask to the target grid
3. Apply the regridded mask to set ocean cells to NaN
4. These NaN values propagate through subsequent regridding stages

This approach prevents issues where bilinear interpolation would otherwise blend land and ocean values near coastlines.

### Data Format

- **NetCDF**: Intermediate regridded data (efficient for sequential writes)
- **Zarr**: Training and bias adjustment (efficient for parallel random access)
- **Output**: Final downscaled data in Zarr format; convert to NetCDF as needed
