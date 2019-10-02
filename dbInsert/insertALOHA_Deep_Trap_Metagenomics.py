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
tableName = 'tblAloha_Deep_Trap_Omics'
rawFilePath = cfgv.rep_hot_raw
rawFileName = 'ALOHA_DeepTrap_omics_ED_Sept25_corr.xlsx'



def makeAloha_Deep_Trap_Omics(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.convertYYYYMMDD(df, 'start_time')
    df = ip.convertYYYYMMDD(df, 'end_time')

    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeAloha_Deep_Trap_Omics(rawFilePath, rawFileName, tableName)

export_path = makeAloha_Deep_Trap_Omics(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
