

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
tableName = 'tblModis_PAR'
rawFilePath = cfgv.rep_MODIS_PAR_8day_raw
filelist = glob.glob(rawFilePath + '*.nc')
server = 'Rainier'

# exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
# prefix = tableName
# export_path = '%s%s.csv' % (exportBase, prefix)
# ############################
# ############################

usecols = ['par']



def makeMODIS_PAR(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df, xdf = ip.netcdf_2_dataframe(path, usecols = usecols)
    df['time'] = pd.to_datetime(os.path.basename(rawFileName).split('_')[2].split('.')[0],format = '%Y%j')
    # df['time'] = pd.to_datetime(xdf.attrs['time_coverage_start']).strftime('%Y-%m-%d %H:%M:%S')
    ip.renameCol(df,'latitude', 'lat')
    ip.renameCol(df,'longitude', 'lon')
    ip.renameCol(df,'par', 'PAR')

    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df = ip.reorderCol(df, ['time','lat','lon', 'PAR'])
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    print('export path: ' ,export_path)
    return export_path


# rawFileName = filelist[0]
# export_path = makeMODIS_PAR(rawFilePath, os.path.basename(rawFileName), tableName)
# iF.toSQLbcp(export_path, tableName,server)
for rawFileName in filelist:
    print(os.path.basename(rawFileName))
    export_path = makeMODIS_PAR(rawFilePath, os.path.basename(rawFileName), tableName)
    iF.toSQLbcp(export_path, tableName,server)
