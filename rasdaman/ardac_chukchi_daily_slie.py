from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def ardac_chukchi_daily_slie(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/landfast_sea_ice_chukchi_daily/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    # why two ingests? one for WMS, and one for WCS only
    ingest_tasks.run_ingest(
        ingest_directory, ingest_file="ingest.json", conda_env="hydrology"
    )
    ingest_tasks.run_ingest(
        ingest_directory, ingest_file="ingest_wcs_only.json", conda_env="hydrology"
    )


if __name__ == "__main__":
    ardac_chukchi_daily_slie.serve(
        name="Rasdaman Coverages: ardac_chukchi_daily_slie, ardac_chukchi_daily_slie_wcs",
        tags=["ARDAC", "Landfast Sea Ice"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/landfast_sea_ice_chukchi_daily/",
        },
    )
