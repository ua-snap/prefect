import os
from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def air_thawing_index_Fdays(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/",
    source_directory="/opt/rasdaman-storage/coverage_data/air_thawing_index_Fdays/",
    zip_file="air_thawing_index.zip",
    python_script="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/merge.py",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    if not os.path.exists(os.path.join(source_directory, "air_thawing_index_Fdays.nc")):
        ingest_tasks.unzip_files(source_directory, zip_file)

        ingest_tasks.run_python_script(
            python_script,
            source_directory,
            os.path.join(source_directory, "air_thawing_index"),
        )

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    air_thawing_index_Fdays.serve(
        name="Rasdaman Coverage: air_thawing_index_Fdays",
        tags=["Thawing Index", "Degree Days"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/",
            "source_directory": "/opt/rasdaman-storage/coverage_data/air_thawing_index_Fdays/",
            "zip_file": "air_thawing_index.zip",
            "python_script": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/air_thawing_index_Fdays/merge.py",
        },
    )
