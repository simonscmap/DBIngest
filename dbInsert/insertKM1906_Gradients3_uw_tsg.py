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
tableName = 'tblKM1906_Gradients3_uw_tsg'
rawFilePath = cfgv.rep_gradients_3_raw
rawFileName = 'G3thslRaphael.dat'


def makeKM1906_Gradients3_uw_tsg(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_csv(path,  delim_whitespace=True)
    df = ip.reorderCol(df,['time','lat','lon','SST', 'salinity', 'flag'])
    #selecting non-flagged values ~3% removed
    df = df[df['flag'] == 22]
    df = ip.removeColumn(['flag'], df)
    df = ip.removeMissings(['time','lat', 'lon'], df)

    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)

    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    print('export path: ' ,export_path)
    return export_path

# df = makeKM1906_Gradients3_uw_tsg(rawFilePath, rawFileName, tableName)


export_path = makeKM1906_Gradients3_uw_tsg(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
