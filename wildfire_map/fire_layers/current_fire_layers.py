import sys
from prefect import flow
from datetime import datetime

# Tries to run the tasks as a local module
# first (works for update_wildfire_map.py main workflow).
# If that fails, it will try to import them
# as a package (works for the single workflow contained in this file).
try:
    from .fire_layer_tasks import *
except ImportError:
    from fire_layer_tasks import *

# Add shared_tasks to the system path
sys.path.append("..")
from shared_tasks import install_conda_environment


@flow(log_prints=True)
def current_fire_layers(
    working_directory,
    script_name,
    shapefile_output_directory,
    conda_local_environment=False,
):
    try:
        install_conda_environment(
            "fire_map",
            f"{working_directory}/fire_layers/environment.yml",
            conda_local_environment,
        )

        execute_local_script(
            f"{working_directory}/fire_layers/{script_name}",
            shapefile_output_directory,
            "fire_map",
            conda_local_environment,
        )
        return {"updated": datetime.now().strftime("%Y%m%d%H"), "succeeded": True}
    except Exception as e:
        return {
            "updated": datetime.now().strftime("%Y%m%d%H"),
            "succeeded": False,
            "error": str(e),
        }


if __name__ == "__main__":
    current_fire_layers.serve(
        name="Update Current Fire Layers (Fire Points / Polygons, Lightning, and MODIS Hotspots)",
        tags=["current_fire_layers", "wildfire_map"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "get_current_fire_layers.py",
            "shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers",
            "conda_local_environment": False,
        },
    )
