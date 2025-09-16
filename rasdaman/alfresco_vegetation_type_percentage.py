from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def alfresco_vegetation_type_percentage(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/vegetation_type_pct/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    alfresco_vegetation_type_percentage.serve(
        name="Rasdaman Coverage: alfresco_vegetation_type_percentage",
        tags=["ALFRESCO", "Vegetation Type Percentage"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/vegetation_type_pct/",
        },
    )
