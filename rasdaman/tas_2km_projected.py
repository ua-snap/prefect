from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def tas_2km_projected(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_2km_projected/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    tas_2km_projected.serve(
        name="Rasdaman Coverage: tas_2km_projected",
        tags=["IEM", "Temperature", "2km"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_2km_projected/",
        },
    )
