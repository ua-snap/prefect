from prefect import flow
import os
import ingest_tasks


@flow(log_prints=True)
def cmip6_fwi(
    branch_name="cmip6_fwi_test",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_fwi",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    # this function will ingest all the CMIP fire weather JSON files in the ingest directory

    ingest_files = [
        f
        for f in os.listdir(ingest_directory)
        if os.path.isfile(os.path.join(ingest_directory, f)) and f.endswith(".json")
    ]

    for ingest_file in ingest_files:
        ingest_tasks.run_ingest(ingest_directory, ingest_file)


if __name__ == "__main__":
    cmip6_fwi.serve(
        name="Rasdaman Coverages: cmip6_dc, cmip6_dmc, cmip6_ffmc, cmip6_bui, cmip6_fwi, cmip6_isi",
        tags=["CMIP6", "CFFDRS", "Fire Weather Index"],
        parameters={
            "branch_name": "cmip6_fwi_test",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/cmip6_fwi",
        },
    )
