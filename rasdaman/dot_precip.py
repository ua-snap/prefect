from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def dot_precip(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/dot_precip/",
    source_directory="/CKAN_Data/Base/Other/DOT_Precipitation/",
    destination_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/dot_precip/undiff/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount()

    ingest_tasks.copy_data_from_nfs_mount(source_directory, destination_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    dot_precip.serve(
        name="Rasdaman Coverage: dot_precip",
        tags=["Precipitation"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/dot_precip/",
            "source_directory": "/CKAN_Data/Base/Other/DOT_Precipitation/",
            "destination_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/dot_precip/undiff/",
        },
    )
