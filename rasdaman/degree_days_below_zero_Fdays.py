from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def degree_days_below_zero_Fdays(
    branch_name,
    working_directory,
    ingest_directory,
    source_file,
    zip_file,
    python_script,
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_file, ingest_directory)

    ingest_tasks.unzip_files(ingest_directory, zip_file)

    ingest_tasks.run_python_script(python_script, ingest_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    degree_days_below_zero_Fdays.serve(
        name="Rasdaman Coverage: degree_days_below_zero_Fdays",
        tags=["Below Zero", "Degree Days"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero_Fdays/",
            "source_file": "/workspace/Shared/Tech_Projects/Degree_Days_NCAR12km/degree_days_below_zero.zip",
            "zip_file": "degree_days_below_zero.zip",
            "python_script": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero_Fdays/merge.py",
        },
    )
