from prefect import flow
from prefect.blocks.system import Secret
from .smokey_bear_tasks import *


@flow(log_prints=True)
def snow_cover_layer(
    working_directory,
    script_name,
):
    # This is a encrypted secret block on the Prefect server that contains the password
    admin_password = Secret.load("smokey-bear-admin-password")

    check_for_admin_pass(f"{working_directory}/smokey_bear/", admin_password.get())

    install_conda_environment(
        "smokeybear", f"{working_directory}/smokey_bear/environment.yml"
    )

    execute_local_script(f"{working_directory}/smokey_bear/{script_name}")


if __name__ == "__main__":
    snow_cover_layer.serve(
        name="Update Snow Cover Layer",
        tags=["snow_cover_layer"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "update_snow_cover.sh",
        },
    )
