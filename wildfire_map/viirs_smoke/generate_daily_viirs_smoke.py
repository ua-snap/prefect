from prefect import flow
from .viirs_smoke_tasks import *
from datetime import datetime


@flow(log_prints=True)
def generate_viirs_smoke(working_directory, output_directory):
    """
    Generate the daily VIIRS smoke layer for the wildfire map.

    This Prefect flow installs the required Conda environment and executes
    scripts to create ADP and AOD smoke layers. The results are saved to the
    specified output directory.

    Args:
        working_directory (str): The path to the working directory containing
            the necessary scripts and environment configuration.
        output_directory (str): The path to the directory where the output
            files will be saved.

    Returns:
        dict: A dictionary containing the following keys:
            - "updated" (str): A timestamp in the format YYYYMMDDHH indicating
              when the flow was executed.
            - "succeeded" (bool): True if the flow completed successfully,
              False otherwise.
            - "error" (str, optional): An error message if the flow failed.
    """
    try:
        install_conda_environment("viirs_smoke", f"{working_directory}/environment.yml")

        execute_local_script(
            f"{working_directory}/adp/create_adp_smoke.py", output_directory
        )
        execute_local_script(
            f"{working_directory}/aod/create_aod_smoke.py", output_directory
        )
        return {"updated": datetime.now().strftime("%Y%m%d%H"), "succeeded": True}
    except Exception as e:
        return {
            "updated": datetime.now().strftime("%Y%m%d%H"),
            "succeeded": False,
            "error": str(e),
        }


if __name__ == "__main__":
    generate_viirs_smoke.serve(
        name="Generate Daily VIIRS Smoke Layer for Wildfire Map",
        tags=["VIIRS Smoke", "Wildfire Map"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map/viirs_smoke",
            "output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers/",
        },
    )
