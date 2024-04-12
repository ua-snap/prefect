from prefect import flow
import paramiko
import ingest_tasks

# Define your SSH parameters
ssh_host = "datacubes.earthmaps.io"
ssh_port = 22


@flow(log_prints=True)
def air_freezing_index_Fdays(
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
        # need to run ingest_tasks.merge() here

        ingest_tasks.run_ingest(ssh, ingest_directory, "ingest.json")
    finally:
        ssh.close()


if __name__ == "__main__":
    air_freezing_index_Fdays.serve(
        name="air_freezing_index_Fdays",
        tags=["air_freezing_index_Fdays"],
        parameters={
            "ssh_username": "cparr4",
            "ssh_private_key_path": "/Users/cparr4/.ssh/id_rsa",
            "branch_name": "degree_days_prefect_remix",
            "working_directory": "/opt/rasdaman/user_data/cparr4/",
            "ingest_directory": "/opt/rasdaman/user_data/cparr4/rasdaman-ingest/arctic_eds/degree_days/air_freezing_index_Fdays/",
            "source_directory": "/CKAN_Data/Base/AK_12km/degree_days/",
            "destination_directory": "/opt/rasdaman/user_data/cparr4/rasdaman-ingest/arctic_eds/degree_days/air_freezing_index_Fdays/zipped/",
        },
    )
