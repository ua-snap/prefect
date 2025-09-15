from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def alfresco_vegetation_mode_statistic(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/mode_vegetation_type/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    alfresco_vegetation_mode_statistic.serve(
        name="Rasdaman Coverage: alfresco_vegetation_mode_statistic",
        tags=["ALFRESCO", "Vegetation Mode"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/mode_vegetation_type/",
        },
    )
