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
tableName = 'tblKOK1806_HOT_LAVA_Dyhrman_Omics'
rawFilePath = cfgv.rep_HOTLAVA_raw
rawFileName = 'Dyhrman__HOTLAVA_complete.xlsx'



def makeKOK1806_HOT_LAVA_Dyhrman_Omics(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df['time'] =  pd.to_datetime(df['time'], format='%m/%d/%YT%H:%M:%S')

    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeKOK1806_HOT_LAVA_Dyhrman_Omics(rawFilePath, rawFileName, tableName)



export_path = makeKOK1806_HOT_LAVA_Dyhrman_Omics(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
