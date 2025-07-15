"""Lookup tables for the regridding Prefect flow."""

# use this list to validate variables
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

# the only two frequencies allowed for regridding are monthly and daily
all_freqs = ["mon", "day"]

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
    "E3SM-2-0",
]

all_scenarios = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
