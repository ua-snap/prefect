from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def design_freezing_index(
    branch_name,
    working_directory,
    ingest_directory,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    design_freezing_index.serve(
        name="Rasdaman Coverage: design_freezing_index",
        tags=["Freezing Index", "Design"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/design_indices/design_freezing_index/",
        },
    )
