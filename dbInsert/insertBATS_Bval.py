

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

############################
########### OPTS ###########

server = 'Rainier'
tableName = 'tblBATS_Bottle_Validation'
rawFilePath = cfgv.rep_bats_raw + 'Bottle/'
rawFileName = 'bval_bottle.txt'

# ############################
# ############################

def makeBATS_Bottle_Validation(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_csv(path,sep='\t')
    df.columns = ['cruise_ID', 'date','decimal_year','time_hhmm','lat','lon','depth','temp'
    ,'CTD_salinity','salinity','sigma_theta','oxygen','oxygen_fix_temp','oxygen_anomaly','CO2','alkalinity'
    ,'nitrate_nitrite','nitrite','phosphate','silicate','POC','PON','TOC','TN','bacteria_enumeration'
    ,'POP','total_dissolved_phosphorus','low_level_phosphorus','particulate_biogenic_silica','particulate_lithogenic_silica'
    ,'prochlorococcus','synechococcus','picoeukaryotes','nanoeukaryotes']
    df['cruise_ID'] = df['cruise_ID'].astype(str)
    df['time_hhmm'] = df['time_hhmm'].astype(str).str.zfill(4)
    df['time'] = pd.to_datetime(df['date'].astype(str) + ':' + df['time_hhmm'].astype(str), format = '%Y%m%d:%H%M')
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df = df.replace(-999, '')
    df = ip.removeColumn(['decimal_year','date','time_hhmm'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','temp'
    ,'CTD_salinity','salinity','sigma_theta','oxygen','oxygen_fix_temp','oxygen_anomaly','CO2','alkalinity'
    ,'nitrate_nitrite','nitrite','phosphate','silicate','POC','PON','TOC','TN','bacteria_enumeration'
    ,'POP','total_dissolved_phosphorus','low_level_phosphorus','particulate_biogenic_silica','particulate_lithogenic_silica'
    ,'prochlorococcus','synechococcus','picoeukaryotes','nanoeukaryotes','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeBATS_Bottle_Validation(rawFilePath, rawFileName, tableName)
export_path = makeBATS_Bottle_Validation(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
