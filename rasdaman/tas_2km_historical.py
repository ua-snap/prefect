from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def tas_2km_historical(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_2km_historical/",
    source_directory="/workspace/Shared/Tech_Projects/IEM/tas_2km_historical/",
    data_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_2km_historical/tas_2km_historical/",
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
    tas_2km_historical.serve(
        name="Rasdaman Coverage: tas_2km_historical",
        tags=["IEM", "Temperature", "2km"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_2km_historical/",
            "source_directory": "/workspace/Shared/Tech_Projects/IEM/tas_2km_historical/",
            "data_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_2km_historical/tas_2km_historical/",
        },
    )
