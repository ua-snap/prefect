from prefect import flow
import paramiko
from pathlib import Path

import ardac_nb_functions

# Define your SSH parameters
ssh_host = "atlas13.snap.uaf.edu" #TODO: avoid specifying a compute node here
ssh_port = 22


@flow(log_prints=True)
def generate_indicators(
    ssh_username,
    ssh_private_key_path,
    branch_name,
    working_directory,
):
    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        ardac_nb_functions.clone_github_repository(ssh, branch_name, working_directory)

        ardac_nb_functions.check_for_nfs_mount(ssh, "/home/UA") #TODO: find out if this is the dir to check!!

        ardac_nb_functions.install_conda_environment(
            ssh, "ardac-toolbox", f"{working_directory}/ardac-toolbox/environment.yml"
        )

        ardac_nb_functions.run_ardac_nbs(ssh, working_directory)

    finally:
        ssh.close()


if __name__ == "__main__":
    ssh_username = "jdpaul3"
    ssh_private_key_path = "/Users/joshpaul/.ssh/id_rsa"
    branch_name = "main"
    working_directory = Path(f"/home/UA/jdpaul3/scratch")
    
    generate_ardac_nbs.serve(
        name="generate-ardac-nbs",
        tags=["ARDAC"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "branch_name": branch_name,
            "working_directory": working_directory,
        },
    )