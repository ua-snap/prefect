from prefect import flow
from prefect.blocks.system import Secret
from .smokey_bear_tasks import *


@flow(log_prints=True)
def smokey_bear_layer(
    home_directory,
    working_directory,
    script_name,
):
    # This is a encrypted secret block on the Prefect server that contains the password
    admin_password = Secret.load("smokey-bear-admin-password")

    check_for_admin_pass(f"{home_directory}", admin_password.get())

    install_conda_environment(
        "smokeybear", f"{working_directory}/smokey_bear/environment.yml"
    )

    execute_local_script(f"{working_directory}/smokey_bear/{script_name}")


if __name__ == "__main__":
    smokey_bear_layer.serve(
        name="Update Smokey Bear Layer",
        tags=["smokey_bear_layer", "wildfire_map"],
        parameters={
            "home_directory": "/home/prefect",
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "update_smokey_bear.sh",
        },
    )
