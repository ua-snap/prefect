from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def annual_mean_temp(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/annual_mean_tas/",
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory, "hook_ingest.json")


if __name__ == "__main__":
    annual_mean_temp.serve(
        name="Rasdaman Coverage: annual_mean_temp",
        tags=["Temperature", "Annual Mean"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/annual_mean_tas/",
        },
    )
