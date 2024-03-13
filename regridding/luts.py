"""Lookup tables for the regridding Prefect flow."""

# use this list to validate variables
# these were copied from the transfers/config.py in the cmip6-utils repo and include the WRF variables

all_vars = [
    'clt',
    'evspsbl',
    'hfls',
    'hfss',
    'hus',
    'huss',
    'mrro',
    'mrsol',
    'mrsos',
    'orog',
    'pr',
    'prsn',
    'ps',
    'psl',
    'rlds',
    'rls',
    'rsds',
    'rss',
    'sfcWind',
    'sfcWindmax',
    'sftlf',
    'sftof',
    'siconc',
    'sithick',
    'snd',
    'snw',
    'ta',
    'tas',
    'tasmax',
    'tasmin',
    'tos',
    'ts',
    'tsl',
    'ua',
    'uas',
    'va',
    'vas',
    'zg',
    ]

land_vars = [
    'mrro',
    'mrsol',
    'mrsos',
    'snd',
    'snw',

]

sea_vars = [
    'tos',
    'siconc',
    'sithick',
]

global_vars = [i for i in all_vars if i not in land_vars + sea_vars]

