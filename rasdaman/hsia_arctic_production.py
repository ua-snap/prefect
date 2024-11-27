from prefect import flow
from prefect.blocks.system import Secret
import ingest_tasks


@flow(log_prints=True)
def hsia_arctic_production(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/hsiaa/",
    source_directory="/workspace/Shared/Tech_Projects/Sea_Ice_Atlas/final_products/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(source_directory, ingest_directory)

    ingest_tasks.untar_file(
        f"{ingest_directory}/rasdaman_hsia_arctic_production_tifs.tgz",
        ingest_directory,
    )

    ingest_tasks.run_ingest(ingest_directory, "hsia_ingest_arctic.json")


if __name__ == "__main__":
    hsia_arctic_production.serve(
        name="Rasdamam Coverage: hsia_arctic_production",
        tags=["Sea Ice", "HSIA"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/hsiaa/",
            "source_directory": "/workspace/Shared/Tech_Projects/Sea_Ice_Atlas/final_products/",
        },
    )
