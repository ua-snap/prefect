from prefect import flow
import smokey_bear_tasks

# Define your SSH parameters
ssh_host = "gs.mapventure.org"
ssh_port = 22


@flow(log_prints=True)
def smokey_bear_layer(
    branch_name,
    admin_password,
    working_directory,
    script_name,
):
    smokey_bear_tasks.clone_github_repository(branch_name, working_directory)

    smokey_bear_tasks.check_for_admin_pass(
        f"{working_directory}/smokey-bear/", admin_password
    )

    smokey_bear_tasks.install_conda_environment(
        "smokeybear", f"{working_directory}/smokey-bear/environment.yml"
    )

    smokey_bear_tasks.execute_local_script(
        f"{working_directory}/smokey-bear/{script_name}"
    )


if __name__ == "__main__":
    smokey_bear_layer.serve(
        name="smokey_bear_layer",
        tags=["smokey_bear_layer"],
        parameters={
            "branch_name": "main",
            "admin_password": "GET THIS FROM BITWARDEN",
            "working_directory": "/tmp",
            "script_name": "update_smokey_bear.sh",
        },
    )
