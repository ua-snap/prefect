"""Contains tasks for converting netcdf files to zarr format. This is done for the bias adjustment to 
improve performance and resilience of the pipeline.
"""

from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils


@task
def run_netcdf_zarr_conversion(
    ssh,
    launcher_script,
    conda_env_name,
    netcdf_dir,  # e.g. /beegfs/CMIP6/kmredilla/daily_era5_4km_3338/
    glob_str,  # e.g. t2max/t2max_*.nc
    year_str,  # e.g. t2max/t2max_{year}_era5_4km_3338.nc
    start_year,  # e.g. 1965
    end_year,  # e.g. 2014
    chunks_dict,
    zarr_path,
    slurm_dir,
):
    script_dir = Path(launcher_script).parent
    cmd = (
        f"python {launcher_script} "
        f"--conda_env_name {conda_env_name} "
        f"--script_dir {script_dir} "
        f"--netcdf_dir {netcdf_dir} "
        f"--glob_str {glob_str} "
        f"--year_str {year_str} "
        f"--start_year {start_year} "
        f"--end_year {end_year} "
        f"--chunks_dict {chunks_dict} "
        f"--zarr_path {zarr_path} "
        f"--slurm_dir {slurm_dir}"
    )

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
    if exit_status != 0:
        # this should error if something fails with creating the job
        raise Exception(f"Error in starting the ERA5 processing. Error: {stderr}")

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    print(f"ERA5 processing job submitted! (job ID: {job_ids[0]})")

    return job_ids
