from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def ardac_beaufort_landfast_sea_ice_mmm(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/beaufort_landfast_sea_ice_mmm/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    ardac_beaufort_landfast_sea_ice_mmm.serve(
        name="Rasdaman Coverage: ardac_beaufort_landfast_sea_ice_mmm",
        tags=["ARDAC", "Landfast Sea Ice"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/beaufort_landfast_sea_ice_mmm/",
        },
    )
