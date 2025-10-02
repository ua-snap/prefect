from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def alfresco_relative_flammability_30yr(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/relative_flammability/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    alfresco_relative_flammability_30yr.serve(
        name="Rasdaman Coverage: alfresco_relative_flammability_30yr",
        tags=["ALFRESCO", "Relative Flammability"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/alfresco/relative_flammability/",
        },
    )
