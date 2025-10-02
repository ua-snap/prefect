from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def design_thawing_index(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/design_indices/design_thawing_index/",
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    design_thawing_index.serve(
        name="Rasdaman Coverage: design_thawing_index",
        tags=["Thawing Index", "Design"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/arctic_eds/design_indices/design_thawing_index/",
        },
    )
