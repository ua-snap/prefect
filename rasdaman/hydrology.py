from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def hydrology(
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(
        source_directory,
        ingest_directory,
        only_files=True,
    )

    ingest_tasks.run_ingest(ingest_directory, conda_env="hydrology")


if __name__ == "__main__":
    hydrology.serve(
        name="Rasdaman Coverage: hydrology",
        tags=["ARDAC", "Hydrology"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/hydrology/",
            "source_directory": "/workspace/Shared/Tech_Projects/Hydrology/netcdf/",
        },
    )
