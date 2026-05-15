import os
import tarfile
import boto3
from pathlib import Path
from prefect import task


@task
def create_tarball(source_directory, output_path=None):
    """
    Create a gzipped tarball from a directory.

    Args:
        source_directory: Path to the directory to archive
        output_path: Optional path for the output tarball. If None, creates it in /tmp

    Returns:
        Path to the created tarball
    """
    source_path = Path(source_directory)

    if not source_path.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_directory}")

    if not source_path.is_dir():
        raise NotADirectoryError(f"Source path is not a directory: {source_directory}")

    if output_path is None:
        tarball_name = f"{source_path.name}.tar.gz"
        output_path = Path("/tmp") / tarball_name
    else:
        output_path = Path(output_path)

    print(f"Creating tarball from {source_directory}")
    print(f"Output path: {output_path}")

    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(source_directory, arcname=source_path.name)

    tarball_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Tarball created successfully: {output_path} ({tarball_size_mb:.2f} MB)")

    return str(output_path)


@task
def upload_to_s3(
    file_path,
    bucket_name,
    aws_access_key_id=None,
    aws_secret_access_key=None,
):
    """
    Upload a file to an S3 bucket.

    Args:
        file_path: Path to the file to upload
        bucket_name: Name of the S3 bucket
        aws_access_key_id: AWS access key (optional if using environment variables or IAM roles)
        aws_secret_access_key: AWS secret access key (optional if using environment variables or IAM roles)

    Returns:
        S3 URI of the uploaded file
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")

    print(f"Uploading {file_path} to s3://{bucket_name}/{file_path.name}")

    if aws_access_key_id and aws_secret_access_key:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")

    s3.upload_file(str(file_path), bucket_name, file_path.name)

    s3_uri = f"s3://{bucket_name}/{file_path.name}"
    print(f"Upload complete: {s3_uri}")

    return s3_uri


@task
def cleanup_temp_file(file_path):
    """
    Remove a temporary file.

    Args:
        file_path: Path to the file to remove
    """
    file_path = Path(file_path)

    if file_path.exists():
        print(f"Cleaning up temporary file: {file_path}")
        file_path.unlink()
        print("Cleanup complete")
    else:
        print(f"File not found, skipping cleanup: {file_path}")
