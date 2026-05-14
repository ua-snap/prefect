from prefect import flow
from prefect.blocks.system import Secret
from snapshot_tasks import create_tarball, upload_to_s3, cleanup_temp_file
from datetime import datetime


@flow(log_prints=True)
def create_directory_snapshot(
    source_directory,
    s3_bucket,
):
    """
    Create a gzipped tarball of a directory and upload it to S3.

    Args:
        source_directory: Path to the directory to archive
        s3_bucket: Name of the S3 bucket to upload to

    Returns:
        Status dictionary with operation details
    """
    status = {
        "started": datetime.now().strftime("%Y%m%d%H%M%S"),
        "source_directory": source_directory,
        "s3_bucket": s3_bucket,
    }

    try:
        aws_access_key_id = Secret.load("alaskawildfires-snapshot-access-key").get()
        aws_secret_access_key = Secret.load("alaskawildfires-snapshot-secret-key").get()

        tarball_path = create_tarball(source_directory, f"{source_directory}.tgz")
        status["tarball_path"] = tarball_path

        s3_uri = upload_to_s3(
            tarball_path,
            s3_bucket,
            aws_access_key_id,
            aws_secret_access_key,
        )
        status["s3_uri"] = s3_uri

        cleanup_temp_file(tarball_path)

        status["completed"] = datetime.now().strftime("%Y%m%d%H%M%S")
        status["succeeded"] = True

        print("Snapshot creation completed successfully")
        print(status)

    except Exception as e:
        status["completed"] = datetime.now().strftime("%Y%m%d%H%M%S")
        status["succeeded"] = False
        status["error"] = str(e)
        print(f"Snapshot creation failed: {e}")

    return status


if __name__ == "__main__":
    create_directory_snapshot.serve(
        name="Create Alaska Wildfires Snapshot",
        tags=["wildfire snapshot", "backup"],
        parameters={
            "source_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires",
            "s3_bucket": "alaskawildfires-snapshots",
        },
    )
