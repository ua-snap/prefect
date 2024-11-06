from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def cmip6_indicators(
    branch_name,
    working_directory,
    ingest_directory,
    source_file,
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_file, ingest_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    cmip6_indicators.serve(
        name="Rasdaman Coverage: cmip6_indicators",
        tags=["CMIP6", "Indicators"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_indicators/",
            "source_file": "/workspace/Shared/Tech_Projects/rasdaman_production_datasets/cmip6_indicators/cmip6_indicators.nc",
        },
    )
