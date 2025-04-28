from prefect import flow
from .viirs_smoke_tasks import *
from datetime import datetime


@flow(log_prints=True)
def generate_viirs_smoke(working_directory, output_directory):
    try:
        install_conda_environment("viirs_smoke", f"{working_directory}/environment.yml")

        execute_local_script(
            f"{working_directory}/adp/create_adp_smoke.py --out-dir ", output_directory
        )
        execute_local_script(
            f"{working_directory}/aod/create_aod_smoke.py --out-dir ", output_directory
        )
        return {"updated": datetime.now().strftime("%Y%m%d%H"), "succeeded": True}
    except Exception as e:
        return {
            "updated": datetime.now().strftime("%Y%m%d%H"),
            "succeeded": False,
            "error": str(e),
        }


if __name__ == "__main__":
    generate_viirs_smoke.serve(
        name="Generate Daily VIIRS Smoke Layer for Wildfire Map",
        tags=["VIIRS Smoke", "Wildfire Map"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map/viirs_smoke",
            "output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/",
        },
    )
