from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def hydrology(
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
    destination_directory,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_directory, destination_directory)

    ingest_tasks.install_conda_environment(
        "rasdaman", f"{working_directory}/rasdaman-ingest/ingest_env.yml"
    )

    ingest_tasks.merge_data(ingest_directory)

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    hydrology.serve(
        name="hydrology",
        tags=["hydrology"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/hydrology/",
            "source_directory": "/workspace/Shared/Tech_Projects/Arctic_EDS/project_data/rasdaman_datasets/hydrology/",
            "destination_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/hydrology/geotiffs/",
        },
    )
