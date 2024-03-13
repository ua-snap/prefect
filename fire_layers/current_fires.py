from prefect import flow
import fire_layer_tasks

# Define your SSH parameters
ssh_host = "gs.mapventure.org"
ssh_port = 22


@flow(log_prints=True)
def current_fires(
    working_directory,
    script_name,
):
    fire_layer_tasks.install_conda_environment(
        "fire_map", f"{working_directory}/fire_layers/environment.yml"
    )

    fire_layer_tasks.execute_local_script(
        f"{working_directory}/fire_layers/{script_name}"
    )


if __name__ == "__main__":
    current_fires.serve(
        name="current_fires",
        tags=["current_fires"],
        parameters={
            "working_directory": "/tmp",
            "script_name": "get_current_fires.py",
        },
    )
