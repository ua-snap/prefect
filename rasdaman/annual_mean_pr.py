from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def annual_mean_pr(
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
    destination_directory,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_directory, destination_directory)

    ingest_tasks.run_ingest(ingest_directory, "hook_ingest.json")


if __name__ == "__main__":
    annual_mean_pr.serve(
        name="annual_mean_pr",
        tags=["annual_mean_pr"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/rltorgerson/",
            "ingest_directory": "/opt/rasdaman/user_data/rltorgerson/rasdaman-ingest/arctic_eds/annual_mean_pr/",
            "source_directory": "/workspace/Shared/Tech_Projects/Arctic_EDS/project_data/rasdaman_datasets/annual_precip_totals_mm/",
            "destination_directory": "/opt/rasdaman/user_data/rltorgerson/rasdaman-ingest/arctic_eds/annual_mean_pr/geotiffs/",
        },
    )
