from prefect import flow
from prefect.blocks.system import Secret
import smokey_bear_tasks


@flow(log_prints=True)
def smokey_bear_layer(
    working_directory,
    script_name,
):
    # This is a encrypted secret block on the Prefect server that contains the password
    admin_password = Secret.load("smokey-bear-admin-password")

    smokey_bear_tasks.check_for_admin_pass(
        f"{working_directory}/smokey_bear/", admin_password.get()
    )

    smokey_bear_tasks.install_conda_environment(
        "smokeybear", f"{working_directory}/smokey_bear/environment.yml"
    )

    smokey_bear_tasks.execute_local_script(
        f"{working_directory}/smokey_bear/{script_name}"
    )


if __name__ == "__main__":
    smokey_bear_layer.serve(
        name="smokey_bear_layer",
        tags=["smokey_bear_layer"],
        parameters={
            "working_directory": "/usr/local/prefect",
            "script_name": "update_smokey_bear.sh",
        },
    )
