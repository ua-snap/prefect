from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def ardac_chukchi_daily_slie(
    branch_name,
    working_directory,
    ingest_directory,
    source_file,
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.untar_file(source_file, ingest_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    ardac_chukchi_daily_slie.serve(
        name="Rasdaman Coverage: ardac_chukchi_daily_slie",
        tags=["ARDAC", "Landfast Sea Ice"],
        parameters={
            "branch_name": "ardac_landfast_sea_ice",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/landfast_sea_ice_chukchi_daily/",
            "source_file": "/workspace/Shared/Tech_Projects/landfast_sea_ice/Chukchi_NetCDFs.tar.gz",
        },
    )