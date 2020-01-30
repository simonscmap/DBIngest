

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
tableName = 'tblBATS_Bacteria_Production'
rawFilePath = cfgv.rep_bats_raw + 'Bacteria_Production/'
rawFileName = 'BATS_bacteria_production.txt'


# ############################
# ############################

def makeBATS_Bacteria_Production(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_csv(path,sep='\t')
    df.columns = ['cruise_ID', 'date','decimal_year','lat','lon','depth','salinity','thy1','thy2','thy3','thy']
    df['time'] = pd.to_datetime(df['date'], format = '%Y%m%d')
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df = df.replace(-999, '')
    df = ip.removeColumn(['decimal_year','date'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','salinity','thy1','thy2','thy3','thy','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

export_path = makeBATS_Bacteria_Production(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
