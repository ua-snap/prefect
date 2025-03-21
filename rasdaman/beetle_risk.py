from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def beetle_risk(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/beetles/",
    source_directory="/CKAN_Data/Base/Other/Spruce_Beetle_Risk/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount()

    ingest_tasks.copy_data_from_nfs_mount(source_directory, ingest_directory)

    ingest_tasks.unzip_files(ingest_directory, "risk_class.zip")

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    beetle_risk.serve(
        name="Rasdaman Coverage: beetle_risk",
        tags=["Beetle Risk"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/beetles/",
            "source_directory": "/CKAN_Data/Base/Other/Spruce_Beetle_Risk/",
        },
    )
