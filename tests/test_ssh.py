from prefect import flow
from utils import utils
import paramiko
from pathlib import Path

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@flow(log_prints=True)
def test_ssh_and_clone(
    ssh_username,
    ssh_private_key_path,
    repo_name,
    branch_name,
    scratch_directory,
):
    """Test the SSH functionality and cloning a GitHub repository via SSH."""
    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        utils.clone_github_repository(ssh, repo_name, branch_name, scratch_directory)

    finally:
        ssh.close()


if __name__ == "__main__":
    # prefect parameter inputs
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    scratch_directory = Path(f"/beegfs/CMIP6/snapdata/")

    test_ssh_and_clone.serve(
        name="test_ssh_and_clone",
        tags=["test"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "scratch_directory": scratch_directory,
        },
    )
