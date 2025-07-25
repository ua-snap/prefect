import time
import subprocess

from prefect import flow, task

import ingest_tasks

# this flow can hit multiple ERA5 variables

@task(log_prints=True)
def run_combine_netcdfs_script(ingest_directory: str, variable_name: str):
    """
    Runs the combine_netcdfs.py script for a given variable.
    The script is expected to be in the ingest_directory.
    Usage of the script is: python combine_netcdfs.py <variable_name>
    """
    script_path = f"{ingest_directory}combine_netcdfs.py"
    command = ["python", script_path, variable_name]
    # must activate conda
    command = [
            "bash",
            "-c",
            f"source /opt/miniconda3/bin/activate rasdaman  && {' '.join(command)}",
    ]

    print(f"Executing command: {' '.join(command)}")
    
    # Using check=True will raise a CalledProcessError if the script fails (non-zero exit code)
    result = subprocess.run(
        command,
        #capture_output=True,
        #text=True,
        check=True,
        cwd=ingest_directory
    )
    
    print("NetCDF combination script execution successful.")
    print(f"STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr}")


@flow(log_prints=True)
def ingest_wrf_downscaled_era5_4km(
    branch_name="era5wrf",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/daily_wrf_downscaled_era5/",
    source_directory="/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km/",
    destination_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/daily_wrf_downscaled_era5/",
    era5_variables=None,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount()

    # for each variable, we must
    # copy the data from the backed up source, and untar it and flatten it
    # then we need to combine the data into a single file
    # run the ingest command, these are all in the ingest directory with names like this:
    # t2_min_ingest.json  rh2_mean_ingest.json  seaice_max_ingest.json

    for variable in era5_variables:
        dest_var_dir = f"{destination_directory}/{variable}"
        source_var_dir = f"{source_directory}/{variable}"
        var_ingest_recipe = f"{variable}_ingest.json"

        ingest_tasks.copy_data_from_nfs_mount(source_var_dir, destination_directory)
        ingest_tasks.untar_file(f"{dest_var_dir}/{variable}_era5_4km_archive.tar.gz", ingest_directory, flatten=True, rename=variable)

        run_combine_netcdfs_script(ingest_directory, variable)

        ingest_tasks.run_ingest(ingest_directory, var_ingest_recipe)
        time.sleep(10)

if __name__ == "__main__":
    era5_variables = ["t2_mean", "t2_max", "t2_min", "rh2_mean", "rh2_max", "rh2_min", "wspd10_mean", "wspd10_max", "wdir10_mean", "rainnc_sum", "seaice_max"]

    ingest_wrf_downscaled_era5_4km.serve(
        name="Rasdaman Coverage: ERA5 4km Daily Summaries (era5_4km_daily_$variable)",
        tags=["ARDAC", "ERA5"],
        parameters = {
           "branch_name": "era5wrf",
           "working_directory": "/opt/rasdaman/user_data/snapdata/cp_dev_ras_ingest",
           "ingest_directory": "/opt/rasdaman/user_data/snapdata/cp_dev_ras_ingest/rasdaman-ingest/ardac/daily_wrf_downscaled_era5/",
           "source_directory": "/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km/",
           "destination_directory": "/opt/rasdaman/user_data/snapdata/cp_dev_ras_ingest/rasdaman-ingest/ardac/daily_wrf_downscaled_era5",
           "era5_variables": era5_variables,
            }
    )
