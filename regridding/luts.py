"""Lookup tables for the regridding Prefect flow."""

# use this list to validate variables
# these were copied from the transfers/config.py in the cmip6-utils repo and include the WRF variables

all_vars = [
    "clt",
    "evspsbl",
    "hfls",
    "hfss",
    "hus",
    "huss",
    "mrro",
    "mrsol",
    "mrsos",
    "orog",
    "pr",
    "prsn",
    "ps",
    "psl",
    "rlds",
    "rls",
    "rsds",
    "rss",
    "sfcWind",
    "sfcWindmax",
    "sftlf",
    "sftof",
    "siconc",
    "sithick",
    "snd",
    "snw",
    "ta",
    "tas",
    "tasmax",
    "tasmin",
    "tos",
    "ts",
    "tsl",
    "ua",
    "uas",
    "va",
    "vas",
    "zg",
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
    "MPI-ESM1-2-LR",
    "MRI-ESM2-0",
    "NorESM2-MM",
    "TaiESM1",
]

all_scenarios = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
