from prefect import flow
from .fire_layer_tasks import *
from datetime import datetime


@flow(log_prints=True)
def current_fire_layers(
    debug, working_directory, script_name, shapefile_output_directory
):
    try:
        install_conda_environment(
            "fire_map", f"{working_directory}/fire_layers/environment.yml"
        )

        execute_local_script(
            f"{working_directory}/fire_layers/{script_name}",
            shapefile_output_directory,
            debug=debug,
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
            "debug": "False",
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "get_current_fire_layers.py",
            "shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers",
        },
    )
