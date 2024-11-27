from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def ncar12km_indicators_era_summaries(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/ncar12km_indicators/",
    source_directory="/workspace/Shared/Tech_Projects/ARDAC/project_data/ncar12km_indicators/",
    data_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/ncar12km_indicators/geotiffs/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(
        source_directory,
        data_directory,
        only_files=True,
    )

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    ncar12km_indicators_era_summaries.serve(
        name="Rasdaman Coverage: ncar12km_indicators_era_summaries",
        tags=["Arctic-EDS", "Precipitation"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/ncar12km_indicators/",
            "source_directory": "/workspace/Shared/Tech_Projects/ARDAC/project_data/ncar12km_indicators/",
            "data_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/ncar12km_indicators/geotiffs/",
        },
    )
