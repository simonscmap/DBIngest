import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
sys.path.append('../../')
import config_vault as cfgv
import pandas as pd
import numpy as np
############################
########### OPTS ###########
server = 'Rainier'
tableName = 'tblGlobalDrifterProgram'
rawFilePath = cfgv.rep_global_drifter_program_raw
rawFileName1 = 'buoydata_1_5000.dat'
rawFileName2 = 'buoydata_5001_10000.dat'
rawFileName3 = 'buoydata_10001_15000.dat'
rawFileName4 = 'buoydata_15001_jun19.dat'

def cleanGlobalDrifter(df):
    df[['day','base_hour']] = df['day'].astype(str).str.split('.',expand=True)
    df['base_hour'] = df['base_hour'].astype(str).str.pad(width=2,side='right',fillchar='0')
    df['hour'] = (df['base_hour'].astype(float) / 100.0) * 24.0
    df['time'] = pd.to_datetime(df[['year','month','day','hour']].astype(str), format='%Y-%m-%d %H:%M:%S')
    df = ip.removeColumn(['month','day','year','base_hour','hour'], df)
    df = ip.reorderCol(df,['time','lat','lon','sst', 'velocity_u', 'velocity_v','drifter_speed','lat_uncertinty','lon_uncertinty','sst_uncertinty','drifter_ID'])
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = df[df['velocity_u'] <999]
    df = df[df['velocity_v'] <999]
    df = df[df['drifter_speed'] <999]
    df.loc[df['sst_uncertinty'] == 1000, 'sst'] = np.nan
    df = ip.NaNtoNone(df)
    df = ip.colDatatypes(df)
    df = ip.removeDuplicates(df)
    return df
def makeGlobalDrifterProgram(rawFilePath, tableName):
    # path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    header = ['drifter_ID','month','day','year','lat','lon','sst','velocity_u','velocity_v','drifter_speed', 'lat_uncertinty', 'lon_uncertinty', 'sst_uncertinty']
    df1 = cleanGlobalDrifter(pd.read_csv(rawFilePath + rawFileName1,  delim_whitespace=True, names = header,index_col = False))
    print('df1')
    df2 = cleanGlobalDrifter(pd.read_csv(rawFilePath + rawFileName2,  delim_whitespace=True, names = header,index_col = False))
    print('df2')
    df3 = cleanGlobalDrifter(pd.read_csv(rawFilePath + rawFileName3,  delim_whitespace=True, names = header,index_col = False))
    print('df3')
    df4 = cleanGlobalDrifter(pd.read_csv(rawFilePath + rawFileName4,  delim_whitespace=True, names = header,index_col = False))
    print('df4')

    df = pd.concat([df1,df2,df3,df4])
    print('concat')



    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    ip.mapTo180180(export_path, 'lon')
    print('export path: ' ,export_path)
    return export_path

# df = makeGlobalDrifterProgram(rawFilePath,  tableName)

# export_path = makeGlobalDrifterProgram(rawFilePath, rawFileName, tableName)
export_path = '/mnt/vault/export/db/dbInsert/export/tblGlobalDrifterProgram.csv'
iF.toSQLbcp(export_path, tableName,server)
