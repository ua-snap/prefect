from prefect import task
import logging


# these were copied from the transfers/config.py in the cmip6-utils repo and include the WRF variables

all_vars = [
    "clt",  # cloud area fraction
    "evspsbl",  # evaporation including sublimation and transpiration
    "hfls",  # surface upward latent heat flux
    "hfss",  # surface upward sensible heat flux
    "hus",  # specific humidity
    "huss",  # near surface specific humidity
    "mrro",  # total runoff
    "mrsol",  # moisture in upper portion of soil column
    "mrsos",  # total water content of soil layer
    "orog",  # surface altitude
    "pr",  # precipitation
    "prsn",  # snowfall flux
    "ps",  # surface air pressure
    "psl",  # sea level pressure
    "rlds",  # surface downwelling longwave flux in air
    "rls",  # surface net downward longwave flux
    "rsds",  # surface downwelling shortwave flux_in_air
    "rss",  # surface net downward shortwave flux
    "sfcWind",  # surface wind speed
    "sfcWindmax",  # maximum surface wind speed
    "sftlf",  # percentage of the grid cell occupied by land including lakes
    "sftof",  # sea area percentage
    "siconc",  # sea ice concentration
    "sithick",  # sea ice thickness
    "snd",  # surface snow thickness
    "snw",  # surface snow amount
    "ta",  # air temperature
    "tas",  # near surface air temperature
    "tasmax",  # maximum near surface air temperature
    "tasmin",  # minimum near surface air temperature
    "tos",  # sea surface temperature
    "ts",  # surface temperature
    "tsl",  # soil temperature
    "ua",  # eastward wind
    "uas",  # near surface eastward wind
    "va",  # northward wind
    "vas",  # near surface northwawrd wind
    "zg",  # geopotential height at 500hPa
]

land_vars = [
    "mrro",
    "mrsol",
    "mrsos",
    "snd",
    "snw",
]

sea_vars = [
    "tos",
    "siconc",
    "sithick",
]

global_vars = [i for i in all_vars if i not in land_vars + sea_vars]

all_models = [
    "CESM2",
    "CNRM-CM6-1-HR",
    "EC-Earth3-Veg",
    "GFDL-ESM4",
    "HadGEM3-GC31-LL",
    "HadGEM3-GC31-MM",
    "KACE-1-0-G",
    "MIROC6",
    "MPI-ESM1-2-HR",
    "MRI-ESM2-0",
    "NorESM2-MM",
    "TaiESM1",
]

all_scenarios = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]

# note, these really aren't the ERA5 variables.
# These are the names given during the ERA5 postprocessing and could change...
cmip6_to_era5_vars_lut = {
    "tasmax": "t2max",
    "tasmin": "t2min",
    "tas": "t2",
    "pr": "pr",
}


def cmip6_to_era5_variables(vars_str):
    """Covnert a string of CMIP6 variables to the equivalent ERA5 variables.

    Parameters:
    - vars: a string of variable ids separated by white space (e.g., 'pr tas ta')
    """
    var_list = vars_str.split()
    era5_var_list = [cmip6_to_era5_vars_lut.get(x) for x in var_list]
    if None in era5_var_list:
        raise ValueError(
            f"Some variables are not in the CMIP6 to ERA5 conversion lookup table: {vars_str}"
        )

    era5_vars_str = " ".join(era5_var_list)

    return era5_vars_str


@task
def validate_vars(vars_str, return_list=True):
    """
    Task to validate strings of variables. Variables are checked against the lists in luts.py.
    Parameters:
    - vars: a string of variable ids separated by white space (e.g., 'pr tas ta') or variable group names found in luts.py (e.g. 'land')
    """
    if vars_str == "all":
        var_list = all_vars
        vars_str = " ".join(all_vars)
    else:
        var_list = vars_str.split()
        assert all(x in all_vars for x in var_list), "Variables not valid."

    if return_list:
        return var_list
    else:
        return vars_str


@task
def validate_models(models_str, return_list=True):
    """Task to validate string of models to work on.
    Parameters:
    - models_str: string of models separated by white space (e.g., 'CESM2 GFDL-ESM4')
    """
    if models_str == "all":
        models = all_models
        models_str = " ".join(models)
    else:
        models = models_str.split()
        assert all(x in all_models for x in models), "Models not valid."

    if return_list:
        return models
    else:
        return models_str


@task
def validate_scenarios(scenarios_str, return_list=True):
    """Task to validate string of scenarios to work on.
    Parameters:
    - scenarios_str: string of scenarios separated by white space (e.g., 'historical ssp585')
    """
    if scenarios_str == "all":
        scenarios = all_scenarios
        scenarios_str = " ".join(all_scenarios)
    else:
        scenarios = scenarios_str.split()
        assert all(x in all_scenarios for x in scenarios), "Scenarios not valid."

    if return_list:
        return scenarios
    else:
        return scenarios_str
