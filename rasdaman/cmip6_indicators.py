from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def cmip6_indicators(
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
    destination_directory,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_directory, destination_directory)

    ingest_tasks.merge_data(ingest_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    cmip6_indicators.serve(
        name="cmip6_indicators",
        tags=["cmip6_indicators"],
        parameters={
            "branch_name": "cmip6_indicators",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_indicators/",
            "source_directory": "/workspace/Shared/Tech_Projects/ARDAC/project_data/cmip6_indicators/",
            "destination_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_indicators/CMIP6_Indicators/",
        },
    )
