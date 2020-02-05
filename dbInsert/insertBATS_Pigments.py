

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
tableName = 'tblBATS_Pigments'
rawFilePath = cfgv.rep_bats_raw + 'Pigments/'
rawFileName = 'bats_pigments.txt'

# ############################
# ############################

def makeBATS_Pigments(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_csv(path,sep='\t')
    df.columns = ['cruise_ID', 'date','decimal_year','time_hhmm','lat','lon','depth','pigment1','pigment2','pigment3','pigment4','pigment5','pigment6','pigment7','pigment8','pigment9','pigment10','pigment11','pigment12','pigment13','pigment14','pigment15','pigment16','pigment17','pigment18','pigment19','pigment20','pigment21']
    df['cruise_ID'] = df['cruise_ID'].astype(str)
    df['time_hhmm'] = df['time_hhmm'].astype(str).str.zfill(4)
    df['time'] = pd.to_datetime(df['date'].astype(str) + ':' + df['time_hhmm'].astype(str), format = '%Y%m%d:%H%M')
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df = df.replace(-999, '')
    df = ip.removeColumn(['decimal_year','date','time_hhmm'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','pigment1','pigment2','pigment3','pigment4','pigment5','pigment6','pigment7','pigment8','pigment9','pigment10','pigment11','pigment12','pigment13','pigment14','pigment15','pigment16','pigment17','pigment18','pigment19','pigment20','pigment21','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    return df

    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

df = makeBATS_Pigments(rawFilePath, rawFileName, tableName)
# export_path = makeBATS_Pigments(rawFilePath, rawFileName, tableName)
# iF.toSQLbcp(export_path, tableName,server)
