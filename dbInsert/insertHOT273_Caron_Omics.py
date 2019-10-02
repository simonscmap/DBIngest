import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
sys.path.append('../../')
import config_vault as cfgv
import pandas as pd

############################
########### OPTS ###########
server = 'Rainier'
tableName = 'tblHOT273_Caron_Omics'
rawFilePath = cfgv.rep_HOT273_raw
rawFileName = 'Caron__HOT273_Revised.xlsx'


def makeHOT273_Caron_Omics(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)

    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeHOT273_Caron_Omics(rawFilePath, rawFileName, tableName)

export_path = makeHOT273_Caron_Omics(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
