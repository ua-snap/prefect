from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def crrel_gipl_outputs(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/gipl/",
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory, "ingest_with_nc.json")


if __name__ == "__main__":
    crrel_gipl_outputs.serve(
        name="Rasdaman Coverage: crrel_gipl_outputs",
        tags=["Permafrost", "GIPL"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/gipl/",
        },
    )
