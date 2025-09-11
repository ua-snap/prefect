from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def ardac_beaufort_daily_slie(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/landfast_sea_ice_beaufort_daily/",
    source_file="/workspace/Shared/Tech_Projects/landfast_sea_ice/Beaufort_NetCDFs.tar.gz",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.untar_file(source_file, ingest_directory)

    # why two ingests? one for WMS, and one for WCS only
    ingest_tasks.run_ingest(ingest_directory, ingest_file="ingest.json", conda_env="hydrology")
    ingest_tasks.run_ingest(ingest_directory, ingest_file="ingest_wcs_only.json", conda_env="hydrology")


if __name__ == "__main__":
    ardac_beaufort_daily_slie.serve(
        name="Rasdaman Coverages: ardac_beaufort_daily_slie, ardac_beaufort_daily_slie_wcs",
        tags=["ARDAC", "Landfast Sea Ice"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/landfast_sea_ice_beaufort_daily/",
            "source_file": "/workspace/Shared/Tech_Projects/landfast_sea_ice/Beaufort_NetCDFs.tar.gz",
        },
    )
