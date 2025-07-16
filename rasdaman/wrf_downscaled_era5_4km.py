from prefect import flow
import ingest_tasks

# this flow will have multiple ERA5 variables that are available for ingest
# default can be "t2_mean" but users can specify other (all?) variables
# current list of variables: rainnc_sum,rh2_max,rh2_mean,rh2_min,t2_max,t2_mean,t2_min,wdir10_mean,wspd10_max,wspd10_mean,seaice_max
# we should validate these


@flow(log_prints=True)
def ingest_wrf_downscaled_era5_4km(
    branch_name="era5wrf",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/daily_wrf_downscaled_era5/",
    source_directory="/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km/",
    destination_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/daily_wrf_downscaled_era5/",
    era5_variables=None,
):

    #ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount()

    # for each variable, we'll need to do these steps
    # copy the data from source to the destination directory (actually destination / $variable_name
    # untar the file, names are like t2_mean_era5_4km_archive.tar.gz
    # run the ingest command, these are all in the ingest directory with names like this:
    # rainnc_sum_ingest.json  rh2_mean_ingest.json  seaice_max_ingest.json
    # we need to run the ingest command for each of these files

    # for variable in era5_variables:
    #     ingest_tasks.copy_data_from_nfs_mount(source_directory, destination_directory / variable)

    #     ingest_tasks.untar_file(destination_directory / variable, f"{variable}_era5_4km_archive.tar.gz")

    #     ingest_tasks.run_ingest(ingest_directory / f"{variable}_ingest.json")


if __name__ == "__main__":
    era5_variables = ["t2_mean", "t2_max", "t2_min", "rh2_mean", "rh2_max", "rh2_min", "wspd10_mean", "wspd10_max", "wdir10_mean", "rainnc_sum", "seaice_max"]

    ingest_wrf_downscaled_era5_4km.serve(
        name="WRF Downscaled ERA5 4km Ingest(s)",
        tags=["ARDAC", "ERA5"],
        parameters={
            "branch_name": "era5wrf",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/daily_wrf_downscaled_era5/",
            "source_directory": "/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km/",
            "destination_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/ardac/daily_wrf_downscaled_era5",
            "era5_variables": era5_variables,
        },
    )
