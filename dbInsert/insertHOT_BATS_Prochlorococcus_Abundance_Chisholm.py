
import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd

############################
########### OPTS ###########
tableName = 'tblHOT_BATS_Prochlorococcus_Abundance'
rawFilePath = cfgv.rep_HOT_BATS_Prochlorococcus_Abundance_Chisholm_raw
rawFileName = 'HOTBATSProchlorococcusAbundance_2020-01-27_v1.0.xlsx'
server = 'Rainier'
############################
############################



def makeHOT_BATS_Prochlorococcus_Abundance(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path, 'data')
    df = ip.renameCol(df,'date', 'time')

    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.convertYYYYMMDD(df,'time')
    df = ip.removeDuplicates(df)
    # return df
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    return export_path

# df = makeHOT_BATS_Prochlorococcus_Abundance(rawFilePath, rawFileName, tableName)
export_path = makeHOT_BATS_Prochlorococcus_Abundance(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
