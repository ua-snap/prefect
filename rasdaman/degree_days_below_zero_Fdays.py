import os
from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def degree_days_below_zero_Fdays(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero_Fdays/",
    source_directory="/opt/rasdaman-storage/coverage_data/degree_days_below_zero_Fdays/",
    zip_file="degree_days_below_zero.zip",
    python_script="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero_Fdays/merge.py",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    if not os.path.exists(
        os.path.join(source_directory, "degree_days_below_zero_Fdays.nc")
    ):
        ingest_tasks.unzip_files(source_directory, zip_file)

        ingest_tasks.run_python_script(
            python_script, source_directory, "degree_days_below_zero"
        )

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    degree_days_below_zero_Fdays.serve(
        name="Rasdaman Coverage: degree_days_below_zero_Fdays",
        tags=["Below Zero", "Degree Days"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero_Fdays/",
            "source_directory": "/opt/rasdaman-storage/coverage_data/degree_days_below_zero_Fdays/",
            "zip_file": "degree_days_below_zero.zip",
            "python_script": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/degree_days_below_zero_Fdays/merge.py",
        },
    )
