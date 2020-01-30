

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
import datetime
############################
########### OPTS ###########
tableName = 'tblBATS_CTD'
rawFilePath = cfgv.rep_bats_raw + 'CTD/'
filelist = glob.glob(rawFilePath + '*.xls')
server = 'Rainier'


exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
prefix = tableName
export_path = '%s%s.csv' % (exportBase, prefix)

# ############################
# ############################

master_df = pd.DataFrame(columns = ['time','lat','lon','depth','pressure','temperature','conductivity','salinity','dissolved_oxygen','beam_attenuation_coefficient','fluorescence','PAR','cruise_ID'])

def makeBATS_CTD(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName.split('.')[0])
    df = pd.read_excel(path)
    if len(df.columns) == 14:
        df.columns = ['cruise_ID', 'decimal_year', 'date','lat','lon','pressure','depth','temperature','conductivity','salinity','dissolved_oxygen','beam_attenuation_coefficient','fluorescence','PAR']
        df.drop('date',axis=1,inplace=True)
    else:
        df.columns = ['cruise_ID', 'decimal_year', 'lat','lon','pressure','depth','temperature','conductivity','salinity','dissolved_oxygen','beam_attenuation_coefficient','fluorescence','PAR']

    df['year'] = pd.to_datetime(df['decimal_year'].astype(str).str.split('.',n=1,expand=True)[0], format = '%Y')
    df['decimal_date'] = '.' + df['decimal_year'].astype(str).str.split('.',n=1,expand=True)[1]
    df['leap_year_mask'] = df['year'].dt.is_leap_year
    df['day_of_year'] = np.where(df['leap_year_mask'] ==True, df['decimal_date'].astype(float) * 366.0, df['decimal_date'].astype(float) * 365.0)
    df['timedelta'] = [datetime.timedelta(dayofyear) for dayofyear in df['day_of_year']]
    df['time'] = df['year'] + df['timedelta']
    df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
    df = df[df['lon'] != 0] # removes weird 0,0 lat/lon vals

    df = df.replace(-999, '')
    df = ip.removeColumn(['decimal_year','year','decimal_date','leap_year_mask','day_of_year','timedelta',], df)
    df = ip.reorderCol(df,['time','lat','lon','depth','pressure','temperature','conductivity','salinity','dissolved_oxygen','beam_attenuation_coefficient','fluorescence','PAR','cruise_ID'])
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    return df



def toSQL(master_df):
    export_path = '%s%s.csv' % (exportBase, prefix)
    master_df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(master_df, export_path, 'time', 'lat', 'lon','depth')
    print(export_path)
    iF.toSQLbcp(export_path, tableName,server)


# rawFileName = '/mnt/vault/observation/in-situ/station/bats/rep/CTD/b10319_ctd.xls'
# df = makeBATS_CTD(rawFilePath, os.path.basename(rawFileName), tableName)
for rawFileName in filelist:
    print(rawFileName)
    df = makeBATS_CTD(rawFilePath, os.path.basename(rawFileName), tableName)
    master_df = master_df.append(df)
#
toSQL(master_df)
