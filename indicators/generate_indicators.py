from prefect import flow
import paramiko
from pathlib import Path

import indicator_functions

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@flow(log_prints=True)
def generate_indicators(
    ssh_username,
    ssh_private_key_path,
    branch_name,
    working_directory,
    indicators,
    models,
    scenarios,
    input_dir,
):
    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        indicator_functions.clone_github_repository(ssh, branch_name, working_directory)

        indicator_functions.check_for_nfs_mount(ssh, "/import/beegfs")

        indicator_functions.install_conda_environment(
            ssh, "cmip6-utils", f"{working_directory}/cmip6-utils/environment.yml"
        )

        indicator_functions.create_and_run_slurm_script(
            ssh, indicators, models, scenarios, working_directory, input_dir
        )

        job_ids = indicator_functions.get_job_ids(ssh, ssh_username)

        indicator_functions.wait_for_jobs_completion(ssh, job_ids)

        indicator_functions.qc(ssh, working_directory, input_dir)

    finally:
        ssh.close()


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    branch_name = "main"
    working_directory = Path(f"/import/beegfs/CMIP6/snapdata/")
    indicators = "rx1day"
    models = "CESM2 GFDL-ESM4 TaiESM1"
    scenarios = "historical ssp126 ssp245 ssp370 ssp585"
    input_dir = Path("/import/beegfs/CMIP6/arctic-cmip6/regrid/")

    generate_indicators.serve(
        name="generate-indicators",
        tags=["CMIP6 Indicators"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "branch_name": branch_name,
            "working_directory": working_directory,
            "indicators": indicators,
            "models": models,
            "scenarios": scenarios,
            "input_dir": input_dir,
        },
    )
