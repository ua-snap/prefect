from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def jan_min_mean_max_temp(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/jan_july_tas_stats/jan_min_mean_max_tas/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory, "hook_ingest.json")


if __name__ == "__main__":
    jan_min_mean_max_temp.serve(
        name="Rasdaman Coverage: jan_min_mean_max_temp",
        tags=["MMM", "Temperature", "January"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/jan_july_tas_stats/jan_min_mean_max_tas/",
        },
    )
