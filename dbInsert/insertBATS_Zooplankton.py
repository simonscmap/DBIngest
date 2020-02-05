

import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd
import glob
import xarray as xr
import os.path
import shutil
import numpy as np
import matplotlib.pyplot as plt

############################
########### OPTS ###########

server = 'Rainier'
tableName = 'tblBATS_Zooplankton_Biomass'
rawFilePath = cfgv.rep_bats_raw + 'Zooplankton_Biomass/'
rawFileName = 'BATS_zooplankton.xlsx'

# ############################
# ############################

def dms2dd(degree_col, min_col, sec_col):
    dd_col = degree_col.astype(float) + (min_col.astype(float))/60.0 + (sec_col.astype(float))/(60.0*60.0)
    return dd_col
    # dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
    # return dd

def makeBATS_Zooplankton_Biomass(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_excel(path)
    df.columns = ['cruise_ID', 'date','tow_number','lat_degrees','lat_minutes','lon_degrees','lon_minutes','time_in','time_out','duration_minutes','depth','volume_water','sieve_size','wet_weight','dry_weight','wet_weight_vol_water_ratio','dry_weight_vol_water_ratio','total_wet_weight_volume_all_size_fractions_ratio',
    'total_dry_weight_volume_all_size_fractions_ratio','wet_weight_vol_water_ratio_200m_depth','dry_weight_vol_water_ratio_200m_depth','total_wet_weight_volume_all_size_fractions_ratio_200m_depth','total_dry_weight_volume_all_size_fractions_ratio_200m_depth']
    df['lat_minutes'], df['lat_seconds'] = df['lat_minutes'].astype(str).str.split('.').str
    df['lon_minutes'], df['lon_seconds'] = df['lon_minutes'].astype(str).str.split('.').str
    df['lat'] = dms2dd(df['lat_degrees'],df['lat_minutes'],df['lat_seconds'])
    df['lon'] = dms2dd(df['lon_degrees'],df['lon_minutes'],df['lon_seconds']) * -1.0

    df['cruise_ID'] = df['cruise_ID'].astype(str)
    df['time'] = pd.to_datetime(df['date'].astype(str), format = '%Y%m%d')
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df = df.replace(-999, '')

    df = ip.removeColumn(['date','lat_degrees','lat_minutes','lat_seconds','lon_degrees','lon_minutes','lon_seconds'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','time_in','time_out','duration_minutes','volume_water','sieve_size','wet_weight','dry_weight','wet_weight_vol_water_ratio','dry_weight_vol_water_ratio','total_wet_weight_volume_all_size_fractions_ratio',
      'total_dry_weight_volume_all_size_fractions_ratio','wet_weight_vol_water_ratio_200m_depth','dry_weight_vol_water_ratio_200m_depth','total_wet_weight_volume_all_size_fractions_ratio_200m_depth','total_dry_weight_volume_all_size_fractions_ratio_200m_depth','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    # return df

    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeBATS_Zooplankton_Biomass(rawFilePath, rawFileName, tableName)
export_path = makeBATS_Zooplankton_Biomass(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
