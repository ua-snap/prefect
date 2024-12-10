from prefect import flow
from prefect.blocks.system import Secret
import hsia_tasks


@flow(log_prints=True)
def hsia(
    years,
    home_directory,
    working_directory,
    source_tar_file,
    tif_directory,
    conda_env,
):

    try:
        nsidc_credentials = Secret.load("nsidc-netrc-credentials")

        hsia_tasks.check_for_netrc_file(home_directory, nsidc_credentials.get())

        hsia_tasks.check_for_nfs_mount("/workspace/Shared")

        hsia_tasks.copy_data_from_nfs_mount(source_tar_file, f"{working_directory}")

        hsia_tasks.untar_file(
            f"{working_directory}rasdaman_hsia_arctic_production_tifs.tgz",
            f"{working_directory}/rasdaman_hsia_arctic_production_tifs",
        )

        # Year can be a list
        if type(years) == list:
            for year in years:
                hsia_tasks.download_new_nsidc_data(year)
                hsia_tasks.generate_annual_sea_ice_geotiffs(
                    year,
                    tif_directory,
                )
        else:
            # Only a single year was requested
            year = years

            hsia_tasks.download_new_nsidc_data(year)

            hsia_tasks.generate_annual_sea_ice_geotiffs(year, tif_directory, conda_env)

        hsia_tasks.tar_directory(
            tif_directory,
            f"{tif_directory}.tgz",
        )

        hsia_tasks.copy_tarfile_to_storage_server(
            f"{tif_directory}.tgz",
            source_tar_file,
        )

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":

    hsia.serve(
        name="Update Annual Sea Ice GeoTIFFs for ingest into Rasdaman",
        tags=["HSIA", "Sea Ice", "Create New Data"],
        parameters={
            "years": 2024,
            "home_directory": "/home/snapdata/",
            "working_directory": "/opt/rasdaman/user_data/snapdata/hsia_updates/",
            "source_tar_file": "/workspace/Shared/Tech_Projects/Sea_Ice_Atlas/final_products/rasdaman_hsia_arctic_production_tifs.tgz",
            "tif_directory": "/opt/rasdaman/user_data/snapdata/hsia_updates/rasdaman_hsia_arctic_production_tifs",
            "conda_env": "hydrology",
        },
    )
