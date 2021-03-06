import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd

############################
########### OPTS ###########
server = 'Rainier'
tableName = 'tblHL2A_diel_metagenomics'
rawFilePath = cfgv.rep_HL2A_diel_metagenomics_raw
rawFileName = 'HL2A_cmap_omics_ED_Aug28_final.xlsx'




def makeHL2A_diel_metagenomics(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_excel(path,  sep=',',sheet_name='data')
    df = ip.removeLeadingWhiteSpace(df)
    ip.renameCol(df,'Time', 'time')
    df['time'] = df['time'].str.strip("'")
    ip.renameCol(df,'Lat', 'lat')
    ip.renameCol(df,'Long', 'lon')
    ip.renameCol(df,'Depth', 'depth')
    df = ip.removeMissings(['time','lat', 'lon','depth'], df)
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(df, export_path, 'time', 'lat', 'lon', 'depth')
    print('export path: ' ,export_path)
    # return df
    return export_path

# df = makeHL2A_diel_metagenomics(rawFilePath, rawFileName, tableName)
#
export_path = makeHL2A_diel_metagenomics(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
