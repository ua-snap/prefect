import os
from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def heating_degree_days_Fdays(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/heating_degree_days_Fdays/",
    source_directory="/opt/rasdaman-storage/coverage_data/heating_degree_days_Fdays/",
    zip_file="heating_degree_days.zip",
    python_script="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/heating_degree_days_Fdays/merge.py",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    if not os.path.exists(
        os.path.join(source_directory, "heating_degree_days_Fdays.nc")
    ):
        ingest_tasks.unzip_files(ingest_directory, zip_file)

        ingest_tasks.run_python_script(
            python_script, ingest_directory, "heating_degree_days"
        )

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    heating_degree_days_Fdays.serve(
        name="Rasdaman Coverage: heating_degree_days_Fdays",
        tags=["Heating", "Degree Days"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/heating_degree_days_Fdays/",
            "source_directory": "/opt/rasdaman-storage/coverage_data/heating_degree_days_Fdays/",
            "zip_file": "heating_degree_days.zip",
            "python_script": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/degree_days/heating_degree_days_Fdays/merge.py",
        },
    )
