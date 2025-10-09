from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def iem_ar5_2km_taspr_seasonal(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory, ingest_file="ar5_seasonal_ingest.json")


if __name__ == "__main__":
    iem_ar5_2km_taspr_seasonal.serve(
        name="Rasdaman Coverage: iem_ar5_2km_taspr_seasonal",
        tags=["IEM", "Temperature", "Precipitation", "AR5"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/",
        },
    )
