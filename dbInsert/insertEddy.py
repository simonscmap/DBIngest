

import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd
import glob
import xarray as xr
import os.path

############################
########### OPTS ###########
server = 'Rainier'
# server = 'Beast'

tableName = 'tblMesoscale_Eddy'
rawFilePath = cfgv.rep_mesoscale_eddy_raw
rawFileName = 'eddy_trajectory_2.0exp_19930101_20180118.nc'
# ############################
# ############################
usecols = [
'obs',
'amplitude',
'cyclonic_type',
'latitude',
'longitude',
'observation_number',
'speed_average',
'speed_radius',
'time',
'track']

def netcdf_2_dataframe(netcdf_file):
    xdf = xr.open_dataset(netcdf_file)
    df = xdf.to_dataframe()
    df.reset_index(inplace=True)
    return df


def makeMesoscale_Eddy(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = netcdf_2_dataframe(path)
    df = df[usecols]
    ip.renameCol(df,'latitude', 'lat')
    ip.renameCol(df,'longitude', 'lon')
    ip.renameCol(df,'amplitude', 'eddy_A')
    ip.renameCol(df,'speed_average', 'eddy_U')
    ip.renameCol(df,'speed_radius', 'eddy_radius')
    ip.renameCol(df,'cyclonic_type', 'eddy_polarity')
    ip.renameCol(df,'observation_number', 'eddy_age')
    df['time'] = pd.to_datetime(df['time'].astype(str))
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month
    df['day'] = df['time'].dt.day
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df = ip.reorderCol(df, ['time','lat','lon', 'year','month','day','track','eddy_age','eddy_polarity','eddy_radius','eddy_A','eddy_U'])
    df.to_csv(export_path, index=False)
    ip.mapTo180180(export_path, 'lon')
    ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    print('export path: ' ,export_path)
    return export_path

# export_path = '/mnt/vault/export/db/dbInsert/export/tblMesoscale_Eddy.csv'
export_path = makeMesoscale_Eddy(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
