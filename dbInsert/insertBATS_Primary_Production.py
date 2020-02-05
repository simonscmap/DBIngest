

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
tableName = 'tblBATS_Primary_Production'
rawFilePath = cfgv.rep_bats_raw + 'Primary_Production/'
rawFileName = 'BATS_primary_production.txt'
# ############################
# ############################

def makeBATS_Primary_Production(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_csv(path,sep='\t')
    df.columns = ['cruise_ID','yymmdd','decimal_year','lat','lon','depth','temp','salt','lt1','lt2','lt3','dark','t0','pp']
    df['cruise_ID'] = df['cruise_ID'].astype(str)
    df['time'] = pd.to_datetime(df['yymmdd'].astype(str), format = '%Y%m%d')
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df = df.replace(-999, '')
    df = ip.removeColumn(['decimal_year','yymmdd'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','temp','salt','lt1','lt2','lt3','dark','t0','pp','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    # return df

    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeBATS_Primary_Production(rawFilePath, rawFileName, tableName)
export_path = makeBATS_Primary_Production(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
