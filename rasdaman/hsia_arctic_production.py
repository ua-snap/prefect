from prefect import flow
from prefect.blocks.system import Secret
import ingest_tasks


@flow(log_prints=True)
def hsia_arctic_production(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/hsiaa/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory, "hsia_ingest_arctic.json")


if __name__ == "__main__":
    hsia_arctic_production.serve(
        name="Rasdamam Coverage: hsia_arctic_production",
        tags=["Sea Ice", "HSIA"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/hsiaa/",
        },
    )
