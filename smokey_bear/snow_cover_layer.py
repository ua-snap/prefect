from prefect import flow
from prefect.blocks.system import Secret
import smokey_bear_tasks

# Define your SSH parameters
ssh_host = "gs.mapventure.org"
ssh_port = 22


@flow(log_prints=True)
def snow_cover_layer(
    branch_name,
    working_directory,
    script_name,
):
    smokey_bear_tasks.clone_github_repository(branch_name, working_directory)

    # This is a encrypted secret block on the Prefect server that contains the password
    admin_password = Secret.load("smokey-bear-admin-password")

    smokey_bear_tasks.check_for_admin_pass(
        f"{working_directory}/smokey-bear/", admin_password.get()
    )

    smokey_bear_tasks.install_conda_environment(
        "smokeybear", f"{working_directory}/smokey-bear/environment.yml"
    )

    smokey_bear_tasks.execute_local_script(
        f"{working_directory}/smokey-bear/{script_name}"
    )


if __name__ == "__main__":
    snow_cover_layer.serve(
        name="snow_cover_layer",
        tags=["snow_cover_layer"],
        parameters={
            "branch_name": "main",
            "working_directory": "/tmp",
            "script_name": "update_snow_cover.sh",
        },
    )
