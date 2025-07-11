from prefect import flow
from prefect.blocks.system import Secret
from .smokey_bear_tasks import *
from datetime import datetime


@flow(log_prints=True)
def snow_cover_layer(
    home_directory,
    working_directory,
    script_name,
):
    try:
        # This is a encrypted secret block on the Prefect server that contains the password
        admin_password = Secret.load("smokey-bear-admin-password")

        check_for_admin_pass(f"{home_directory}", admin_password.get())

        # This is referenced in Github issue: https://github.com/ua-snap/prefect/issues/67
        #       being found correctly.
        # install_conda_environment(
        #     "smokeybear", f"{working_directory}/smokey_bear/environment.yml"
        # )

        execute_local_script(f"{working_directory}/smokey_bear/{script_name}")
        return {"updated": datetime.now().strftime("%Y%m%d%H"), "succeeded": True}
    except Exception as e:
        return {
            "updated": datetime.now().strftime("%Y%m%d%H"),
            "succeeded": False,
            "error": str(e),
        }


if __name__ == "__main__":
    snow_cover_layer.serve(
        name="Update Snow Cover Layer",
        tags=["snow_cover_layer", "wildfire_map"],
        parameters={
            "home_directory": "/home/snapdata",
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "update_snow_cover.sh",
        },
    )
