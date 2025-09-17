from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def iem_cru_2km_taspr_seasonal(
    branch_name,
    working_directory,
    ingest_directory,
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory, ingest_file="cru_seasonal_ingest.json")


if __name__ == "__main__":
    iem_cru_2km_taspr_seasonal.serve(
        name="Rasdaman Coverage: iem_cru_2km_taspr_seasonal",
        tags=["IEM", "Temperature", "Precipitation", "CRU TS"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/",
        },
    )
