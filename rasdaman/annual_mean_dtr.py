from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def annual_mean_dtr(
    branch_name,
    working_directory,
    ingest_directory,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    annual_mean_dtr.serve(
        name="Rasdaman Coverage: annual_mean_dtr",
        tags=["Annual Mean"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/annual_mean_dtr/",
        },
    )
