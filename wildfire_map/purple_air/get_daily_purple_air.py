from prefect import flow
from prefect.blocks.system import Secret
from purple_air_tasks import *
from datetime import datetime


@flow(log_prints=True)
def purple_air(working_directory, script_name, shapefile_output_directory):
    try:
        install_conda_environment(
            "fire_map", f"{working_directory}/fire_layers/environment.yml"
        )

        purple_air_key = Secret.load("purple-air-key")

        execute_local_script(
            f"{working_directory}/purple_air/{script_name}",
            shapefile_output_directory,
            purple_air_key.get(),
        )
        return {"updated": datetime.now().strftime("%Y%m%d%H"), "succeeded": True}
    except Exception as e:
        return {
            "updated": datetime.now().strftime("%Y%m%d%H"),
            "succeeded": False,
            "error": str(e),
        }


if __name__ == "__main__":
    purple_air.serve(
        name="Update Purple Air Layer for Wildfire Map",
        tags=["current_fire_layers"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "get_purple_air.py",
            "shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers/purple_air_dec/",
        },
    )
