from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def alfresco_vegetation_mode_statistic(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/mode_vegetation_type/",
    source_directory="/workspace/Shared/Tech_Projects/Alaska_IEM/project_data/NCR_ALFRESCO_datasets/alfresco_vegetation_mode_statistic/",
    data_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/mode_vegetation_type/mode_vegetation_geotiffs/",
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
    alfresco_vegetation_mode_statistic.serve(
        name="Rasdaman Coverage: alfresco_vegetation_mode_statistic",
        tags=["ALFRESCO", "Vegetation Mode"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/mode_vegetation_type/",
            "source_directory": "/workspace/Shared/Tech_Projects/Alaska_IEM/project_data/NCR_ALFRESCO_datasets/alfresco_vegetation_mode_statistic/",
            "data_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/mode_vegetation_type/mode_vegetation_geotiffs/",
        },
    )
