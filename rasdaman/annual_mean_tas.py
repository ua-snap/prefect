from prefect import flow
import paramiko
import ingest_tasks

# Define your SSH parameters
ssh_host = "datacubes.earthmaps.io"
ssh_port = 22


@flow(log_prints=True)
def annual_mean_tas(
    ssh_username,
    ssh_private_key_path,
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
    destination_directory,
):
    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        ingest_tasks.clone_github_repository(ssh, branch_name, working_directory)

        ingest_tasks.check_for_nfs_mount(ssh, "/workspace/Shared")

        ingest_tasks.copy_data_from_nfs_mount(
            ssh, source_directory, destination_directory
        )

        ingest_tasks.run_ingest(ssh, ingest_directory, "hook_ingest.json")
    finally:
        ssh.close()


if __name__ == "__main__":
    annual_mean_tas.serve(
        name="annual_mean_tas",
        tags=["annual_mean_tas"],
        parameters={
            "ssh_username": "rltorgerson",
            "ssh_private_key_path": "/Users/rltorgerson/.ssh/id_rsa",
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/rltorgerson/",
            "ingest_directory": "/opt/rasdaman/user_data/rltorgerson/rasdaman-ingest/arctic_eds/annual_mean_tas/",
            "source_directory": "/workspace/Shared/Tech_Projects/Arctic_EDS/project_data/rasdaman_datasets/annual_mean_temp/",
            "destination_directory": "/opt/rasdaman/user_data/rltorgerson/rasdaman-ingest/arctic_eds/annual_mean_tas/geotiffs/",
        },
    )
