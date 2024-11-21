from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def cmip6_monthly(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_common_grid/monthly",
    source_directory="/workspace/Shared/Tech_Projects/rasdaman_production_datasets/cmip6_monthly/",
    data_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_common_grid/monthly/data",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(
        source_directory, data_directory, only_files=True
    )

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    cmip6_monthly.serve(
        name="Rasdaman Coverage: cmip6_monthly",
        tags=["CMIP6", "Monthly"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_common_grid/monthly",
            "source_directory": "/workspace/Shared/Tech_Projects/rasdaman_production_datasets/cmip6_monthly/",
            "data_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_common_grid/monthly/data",
        },
    )
