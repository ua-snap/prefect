from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def wet_days_per_year(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/wet_days_per_year/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    wet_days_per_year.serve(
        name="Rasdaman Coverage: wet_days_per_year",
        tags=["Arctic-EDS", "Precipitation"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/wet_days_per_year/",
        },
    )
