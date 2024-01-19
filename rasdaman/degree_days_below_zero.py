from prefect import flow
import paramiko
import ingest_tasks

# Define your SSH parameters
ssh_host = "datacubes.earthmaps.io"
ssh_port = 22


@flow(log_prints=True)
def degree_days_below_zero(
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
    degree_days_below_zero.serve(
        name="degree_days_below_zero",
        tags=["degree_days_below_zero"],
        parameters={
            "ssh_username": "rltorgerson",
            "ssh_private_key_path": "/Users/rltorgerson/.ssh/id_rsa",
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/rltorgerson/",
            "ingest_directory": "/opt/rasdaman/user_data/rltorgerson/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero/",
            "source_directory": "/workspace/Shared/Tech_Projects/Arctic_EDS/project_data/rasdaman_datasets/degree_days_below_zero/",
            "destination_directory": "/opt/rasdaman/user_data/rltorgerson/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero/geotiffs/",
        },
    )
