from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def crrel_gipl_outputs(
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
    destination_directory,
):

    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount()

    ingest_tasks.copy_data_from_nfs_mount(source_directory, destination_directory)

    ingest_tasks.unzip_files(destination_directory, "gipl_magt0.5m_cmip5_2021-2120.zip")
    ingest_tasks.unzip_files(destination_directory, "gipl_magt1m_cmip5_2021-2120.zip")
    ingest_tasks.unzip_files(destination_directory, "gipl_magt2m_cmip5_2021-2120.zip")
    ingest_tasks.unzip_files(destination_directory, "gipl_magt3m_cmip5_2021-2120.zip")
    ingest_tasks.unzip_files(destination_directory, "gipl_magt4m_cmip5_2021-2120.zip")
    ingest_tasks.unzip_files(destination_directory, "gipl_magt5m_cmip5_2021-2120.zip")
    ingest_tasks.unzip_files(
        destination_directory, "gipl_magtsurface_cmip5_2021-2120.zip"
    )
    ingest_tasks.unzip_files(
        destination_directory, "gipl_permafrostbase_cmip5_2021-2120.zip"
    )
    ingest_tasks.unzip_files(
        destination_directory, "gipl_permafrosttop_cmip5_2021-2120.zip"
    )
    ingest_tasks.unzip_files(
        destination_directory, "gipl_talikthickness_cmip5_2021-2120.zip"
    )

    ingest_tasks.run_ingest(ingest_directory)


if __name__ == "__main__":
    crrel_gipl_outputs.serve(
        name="crrel_gipl_outputs",
        tags=["crrel_gipl_outputs"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/gipl/",
            "source_directory": "/CKAN_Data/Base/AK_1km/GIPL/",
            "destination_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/gipl/geotiffs/",
        },
    )
