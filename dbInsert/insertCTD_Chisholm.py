
import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd

############################
########### OPTS ###########
tableName = 'tblCTD_Chisholm'
rawFilePath = cfgv.rep_BiGRAPA1_CTDData_Chisholm_raw
rawFileName = 'BiGRAPA1_CTDData_2019-03-19_v1.1.xlsx'
server = 'Rainier'

############################
############################



def makeAMT13_Chisholm(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path, 'data')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.convertYYYYMMDD(df,'time')
    df = ip.removeDuplicates(df)
    print(export_path)
    df.to_csv(export_path, index=False)

    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeAMT13_Chisholm(rawFilePath, rawFileName, tableName)
export_path = makeAMT13_Chisholm(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
