

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
tableName = 'tblBATS_Sediment_Trap'
rawFilePath = cfgv.rep_bats_raw + 'Sediment_Trap/'
rawFileName = 'bats_flux.xlsx'

# ############################
# ############################

def makeBATS_Sediment_Trap(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_excel(path)
    df.columns = ['cruise_ID', 'depth','yymmdd','time_recovery','decimal_year','decimal_year_recovery','lat','lat_recovery','lon','lon_recovery','M1','M2','M3','M_avg','C1','C2','C3','C_avg','N1','N2','N3','N_avg','P1','P2','P3','P_avg','FBC1','FBC2','FBC3','FBC_avg','FBN1','FBN2','FBN3','FBN_avg']

    df['cruise_ID'] = df['cruise_ID'].astype(str)
    df['time'] = pd.to_datetime(df['yymmdd'].astype(str), format = '%Y%m%d')
    df['time_recovery'] = df['time_recovery'].replace(19990001, '')
    # return df
    df['time_recovery'] = pd.to_datetime(df['time_recovery'].astype(str), format = '%Y%m%d', errors='coerce')
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df['lon_recovery'] = df['lon_recovery'] * -1.0 # converts degrees west to -180,180.

    df = df.replace(-999, '')
    df = ip.removeColumn(['yymmdd','decimal_year','decimal_year_recovery'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','time_recovery','lat_recovery', 'lon_recovery','M1','M2','M3','M_avg','C1','C2','C3','C_avg','N1','N2','N3','N_avg','P1','P2','P3','P_avg','FBC1','FBC2','FBC3','FBC_avg','FBN1','FBN2','FBN3','FBN_avg','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)

    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeBATS_Sediment_Trap(rawFilePath, rawFileName, tableName)
export_path = makeBATS_Sediment_Trap(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
