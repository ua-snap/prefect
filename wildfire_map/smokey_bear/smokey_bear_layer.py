import sys
from prefect import flow
from prefect.blocks.system import Secret
from datetime import datetime

# Tries to run the tasks as a local module
# first (works for update_wildfire_map.py main workflow).
# If that fails, it will try to import them
# as a package (works for the single workflow contained in this file).
try:
    from .smokey_bear_tasks import *
except ImportError:
    from smokey_bear_tasks import *

# Add shared_tasks to the system path
sys.path.append("..")
from shared_tasks import install_conda_environment


@flow(log_prints=True)
def smokey_bear_layer(
    home_directory,
    working_directory,
    script_name,
    output_directory,
    conda_local_environment=False,
):
    try:
        # This is a encrypted secret block on the Prefect server that contains the password
        admin_password = Secret.load("smokey-bear-admin-password")

        check_for_admin_pass(f"{home_directory}", admin_password.get())

        install_conda_environment(
            "smokeybear",
            f"{working_directory}/smokey_bear/environment.yml",
            conda_local_environment,
        )

        execute_local_script(
            f"{working_directory}/smokey_bear/{script_name}",
            output_directory,
            "smokeybear",
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
    smokey_bear_layer.serve(
        name="Update Smokey Bear Layer",
        tags=["smokey_bear_layer", "wildfire_map"],
        parameters={
            "home_directory": "/home/snapdata",
            "working_directory": "/usr/local/prefect/wildfire_map",
            "script_name": "update_smokey_bear.sh",
            "output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/",
            "conda_local_environment": False,
        },
    )
