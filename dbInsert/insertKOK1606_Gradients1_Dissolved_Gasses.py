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
tableName = 'tblKOK1606_Gradients1_Dissolved_Gasses'
rawFilePath = cfgv.rep_gradients_1_raw
rawFileName = 'Karl_G1_gases_cmap.xlsx'



def makeKOK1606_Gradients1_Dissolved_Gasses(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    ip.renameCol(df,'Date-Time', 'time')
    ip.renameCol(df,'Latitude', 'lat')
    ip.renameCol(df,'Longitude', 'lon')
    ip.renameCol(df,'Depth', 'depth')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path


# df = makeKOK1606_Gradients1_Dissolved_Gasses(rawFilePath, rawFileName, tableName)

export_path = makeKOK1606_Gradients1_Dissolved_Gasses(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
