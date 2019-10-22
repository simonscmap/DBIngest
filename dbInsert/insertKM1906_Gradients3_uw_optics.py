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
tableName = 'tblKM1906_Gradients3_uway_optics'
rawFilePath = cfgv.rep_gradients_3_raw
rawFileName = 'gradients3_UWoptics.xlsx'



def makeKM1906_Gradients3_uway_optics(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df['time'] = df['time'].str.strip('z') #removes trailing 'z' typo
    df = ip.replaceMissings(-999.000000, df)
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)

    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    print('export path: ' ,export_path)
    return export_path

# df = makeKM1906_Gradients3_uway_optics(rawFilePath, rawFileName, tableName)



export_path = makeKM1906_Gradients3_uway_optics(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
