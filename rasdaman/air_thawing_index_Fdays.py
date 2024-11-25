from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def air_thawing_index_Fdays(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/",
    source_file="/workspace/Shared/Tech_Projects/Degree_Days_NCAR12km/air_thawing_index.zip",
    zip_file="air_thawing_index.zip",
    python_script="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/merge.py",
):
    python_script = f"{python_script} -d {ingest_directory}/air_thawing_index/"

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_file, ingest_directory)

    ingest_tasks.unzip_files(ingest_directory, zip_file)

    ingest_tasks.run_python_script(python_script, ingest_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    air_thawing_index_Fdays.serve(
        name="Rasdaman Coverage: air_thawing_index_Fdays",
        tags=["Thawing Index", "Degree Days"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/",
            "source_file": "/workspace/Shared/Tech_Projects/Degree_Days_NCAR12km/air_thawing_index.zip",
            "zip_file": "air_thawing_index.zip",
            "python_script": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/merge.py",
        },
    )
