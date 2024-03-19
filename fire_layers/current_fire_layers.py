from prefect import flow
import fire_layer_tasks

# Define your SSH parameters
ssh_host = "gs.mapventure.org"
ssh_port = 22


@flow(log_prints=True)
def current_fire_layers(
    debug, working_directory, script_name, shapefile_output_directory
):
    fire_layer_tasks.install_conda_environment(
        "fire_map", f"{working_directory}/fire_layers/environment.yml"
    )

    fire_layer_tasks.execute_local_script(
        f"{working_directory}/fire_layers/{script_name}",
        shapefile_output_directory,
        debug=debug,
    )


if __name__ == "__main__":
    current_fire_layers.serve(
        name="current_fire_layers",
        tags=["current_fire_layers"],
        parameters={
            "debug": "False",
            "working_directory": "/usr/local/prefect",
            "script_name": "get_current_fire_layers.py",
            "shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers",
        },
    )
