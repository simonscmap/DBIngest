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
tableName = 'tblMGL1704_Gradients2_Nutrients'
rawFilePath = cfgv.rep_gradients_2_raw
rawFileName = 'Gradients_2_organic_and_inorganic_nutrients.xlsx'






def makeMGL1704_Gradients2_Nutrients(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df['time'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    df = df.drop(['Time', 'Date'], axis=1)
    ip.renameCol(df,'Latitude', 'lat')
    ip.renameCol(df,'Longitude', 'lon')
    ip.renameCol(df,'Depth', 'depth')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df = ip.reorderCol(df,['time','lat','lon', 'depth','Station','Cast','RosPos','SiO4','NO3_NO2','TN','TON','PO4','TP','TOP','TOC','NH4'])
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path


# df = makeMGL1704_Gradients2_Nutrients(rawFilePath, rawFileName, tableName)

export_path = makeMGL1704_Gradients2_Nutrients(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
