

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
sys.path.append('../summary_stats/')
import summary_stats_func as ssf

############################
########### OPTS ###########
tableName = 'tblOceans_Melting_Greenland_CTD'
rawFilePath = cfgv.rep_oceans_melting_greenland_CTD_raw
filelist = glob.glob(rawFilePath + '*.nc')
server = 'Rainier'


exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
prefix = tableName
export_path = '%s%s.csv' % (exportBase, prefix)

# ############################
# ############################




def makeOMG_Greenland_CTD(rawFilePath, rawFileName, tableName):
    usecols = ['temperature','conductivity','salinity','sound_velocity','density']
    path = rawFilePath + rawFileName
    # print(path)

    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName)
    # print(export_path)
    df, xdf = ip.netcdf_2_dataframe(path, usecols = usecols)
    df = ip.removeColumn(['obs','profile'], df)
    df = ip.reorderCol(df,['time','lat','lon','depth'] + usecols)
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    return df,xdf
    # # ssf.dataframe_describe_write(df, tableName)
    # df.to_csv(export_path, index=False)
    # ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    # print('export path: ' ,export_path)
    # return export_path



for rawFileName in filelist:
    print(rawFileName)
    df,xdf = makeOMG_Greenland_CTD(rawFilePath, os.path.basename(rawFileName), tableName)
    break
    # export_path = makeOMG_Greenland_CTD(rawFilePath, os.path.basename(rawFileName), tableName)
    # print(export_path)
    # iF.toSQLbcp(export_path, tableName,server)
