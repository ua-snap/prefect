from prefect import flow, task
from pathlib import Path
import datetime
import s3fs
import subprocess
from viirs_smoke_tasks import (
    find_jpss_observation_times,
    query_nodd_viirs_adp,
    plot_simple_viirs_adp_smoke,
    create_geotiff,
    run_in_conda_env,
)


@task
def ensure_conda_environment():
    """Task to ensure the conda environment exists and is up to date"""
    # Check if environment exists
    env_check = subprocess.run(
        "source /opt/miniconda3/bin/activate && conda env list | grep viirs_smoke",
        shell=True,
        executable="/bin/bash",
        capture_output=True,
        text=True,
    )

    if env_check.returncode != 0:
        # Environment doesn't exist, create it
        print("Creating viirs_smoke conda environment...")
        env_path = Path(__file__).parent / "environment.yml"
        result = run_in_conda_env(f"conda env create -f {env_path}", env_name="base")
        if result[0] != 0:
            raise Exception(f"Failed to create conda environment: {result[2]}")
    else:
        # Environment exists, update it
        print("Updating viirs_smoke conda environment...")
        env_path = Path(__file__).parent / "environment.yml"
        result = run_in_conda_env(f"conda env update -f {env_path}", env_name="base")
        if result[0] != 0:
            raise Exception(f"Failed to update conda environment: {result[2]}")


@flow
def process_viirs_smoke(
    sat_name: str = "NOAA21",
    observation_date: str = None,
    png_domain: list = [180, 225, 50, 71],  # [W_lon, E_lon, S_lat, N_lat]
    tif_domain: list = [-180, 72, -135, 50],  # [W_lon, N_lat, E_lon, S_lat]
    output_dir: str = None,
):
    """
    Process VIIRS smoke data for Alaska domain.

    Args:
        sat_name: Satellite name ('SNPP', 'NOAA20', 'NOAA21')
        observation_date: Date in 'YYYYMMDD' format
        png_domain: Domain for map image file [W_lon, E_lon, S_lat, N_lat]
        tif_domain: Domain for GeoTIFF file [W_lon, N_lat, E_lon, S_lat]
        output_dir: Directory to save output files
    """
    # Ensure conda environment exists and is up to date
    ensure_conda_environment()

    # Set default observation date to today if not provided
    if observation_date is None:
        observation_date = datetime.datetime.now().strftime("%Y%m%d")

    # Set default output directory if not provided
    if output_dir is None:
        output_dir = Path.cwd() / "data"
    else:
        output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Connect to AWS S3 anonymously
    fs = s3fs.S3FileSystem(anon=True)

    # Process east of Antimeridian
    east_start_times, east_end_times = find_jpss_observation_times(
        observation_date, "45,-180", "75,-130", sat_name
    )
    east_file_list = query_nodd_viirs_adp(
        fs, observation_date, sat_name, east_start_times, east_end_times
    )

    # Process west of Antimeridian (next day in UTC)
    obs_date = datetime.datetime.strptime(observation_date, "%Y%m%d").date()
    tomorrow = obs_date + datetime.timedelta(days=1)
    tomorrow = tomorrow.strftime("%Y%m%d")

    west_start_times, west_end_times = find_jpss_observation_times(
        tomorrow, "45,170", "75,180", sat_name
    )
    west_file_list = query_nodd_viirs_adp(
        fs, tomorrow, sat_name, west_start_times, west_end_times
    )

    # Combine files for both domains
    file_list = east_file_list + west_file_list

    if not file_list:
        print("No VIIRS files found for the specified date and domain")
        return

    # Create VIIRS Smoke ADP map image file
    save_name = plot_simple_viirs_adp_smoke(fs, file_list, png_domain, output_dir)

    # Convert PNG to GeoTIFF
    image_file_path = (output_dir / f"{save_name}.png").as_posix()
    tif_file_path = (output_dir / f"{save_name}.tif").as_posix()
    create_geotiff(tif_file_path, image_file_path, tif_domain)

    return tif_file_path


if __name__ == "__main__":
    process_viirs_smoke.serve(
        name="Update VIIRS Smoke Layer for Wildfire Map",
        tags=["Smoke", "VIIRS"],
        parameters={
            "sat_name": "NOAA21",
            "observation_date": None,
            "png_domain": [180, 225, 50, 71],
            "tif_domain": [-180, 72, -135, 50],
            "output_dir": "/usr/share/geoserver/data_dir/data/alaska_wildfires/viirs_smoke/",
        },
    )
