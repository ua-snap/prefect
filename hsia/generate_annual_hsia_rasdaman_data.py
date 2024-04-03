from prefect import flow
from prefect.blocks.system import Secret
import hsia_tasks


@flow(log_prints=True)
def hsia(
    year,
    home_directory,
    env_path,
    working_directory,
    source_tar_file,
    tif_directory,
):

    try:
        nsidc_credentials = Secret.load("nsidc-netrc-credentials")

        hsia_tasks.check_for_netrc_file(home_directory, nsidc_credentials.get())

        hsia_tasks.check_for_nfs_mount("/workspace/Shared")

        hsia_tasks.copy_data_from_nfs_mount(
            source_tar_file, f"{working_directory}/hsia"
        )

        hsia_tasks.untar_file(
            f"{working_directory}/hsia/rasdaman_hsia_arctic_production_tifs.tgz",
            f"{working_directory}/hsia/",
        )

        hsia_tasks.download_new_nsidc_data(year)

        hsia_tasks.generate_annual_sea_ice_geotiffs(
            year,
            env_path,
            tif_directory,
        )

        hsia_tasks.tar_directory(
            f"{working_directory}/hsia/rasdaman_hsia_arctic_production_tifs",
            f"{working_directory}/hsia/rasdaman_hsia_arctic_production_tifs.tgz",
        )

        hsia_tasks.copy_tarfile_to_nfs_mount(
            f"{working_directory}/hsia/rasdaman_hsia_arctic_production_tifs.tgz",
            source_tar_file,
        )

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":

    hsia.serve(
        name="Update Annual Sea Ice GeoTIFFs for ingest into Rasdaman",
        tags=["hsia", "sea ice"],
        parameters={
            "year": 2022,
            "home_directory": "/home/snapdata/",
            "env_path": "/home/snapdata/.conda/envs/rasdaman/share/",  # This is the path to the environment's shared libraries (GDAL & proj4)
            "working_directory": "/home/snapdata/",
            "source_tar_file": "/workspace/Shared/Tech_Projects/Sea_Ice_Atlas/final_products/rasdaman_hsia_arctic_production_tifs.tgz",
            "tif_directory": "/home/snapdata/hsia/rasdaman_hsia_arctic_production_tifs/",
        },
    )
