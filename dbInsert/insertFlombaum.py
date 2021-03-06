import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd

############################
########### OPTS ###########

tableName = 'tblFlombaum'
rawFilePath = cfgv.rep_flombaum_raw
rawFileName = 'flombaum.xlsx'
############################



def makeFlombaum(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.convertYYYYMMDD(df)
    df = ip.addIDcol(df)
    df = ip.removeDuplicates(df)
    df['lon'] = df['lon'].abs()
    df.to_csv(export_path, index=False)
    ip.mapTo180180(export_path, 'lon')
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path



export_path = makeFlombaum(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName)
