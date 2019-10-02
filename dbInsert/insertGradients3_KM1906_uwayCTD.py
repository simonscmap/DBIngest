import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
sys.path.append('../../')
import config_vault as cfgv
import pandas as pd
import glob
import os
import numpy as np
############################
########### OPTS ###########
server = 'Rainier'
tableName = 'tblKM1906_Gradients3_uwayCTD'
rawFilePath = cfgv.rep_gradients_3_raw
rawFileName = 'KM1906_uCTD_meta.csv'



def makeKM1906_Gradients3_uwayCTD(rawFilePath, rawFileName, tableName):
    path = rawFilePath + rawFileName
    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df = pd.read_csv(path,  sep=',')
    df.insert(0, 'CTD_num', range(1, 1 + len(df)))

    ctd_list = np.sort(glob.glob(rawFilePath + 'uway_ctd/' + '*.csv*'))
    concat_df = pd.DataFrame(columns = ['depth','temperature','salinity','adjusted_salinity','density','fluorescence','turbidity','oxygen','oxygen_saturation','CTD_num','lat','lon','time_hst'])
    for ctd in ctd_list:
        ctd_num = os.path.basename(ctd)[-6:-4]
        ctd_df = pd.read_csv(ctd,sep=',')
        ctd_df['CTD_num'] = int(ctd_num)
        ctd_df.columns = ['depth','temperature','salinity', 'adjusted_salinity','density','fluorescence','turbidity','oxygen','oxygen_saturation','CTD_num']
        ctd_df_merge = pd.merge(ctd_df,df, how='left', on='CTD_num')
        concat_df = concat_df.append(ctd_df_merge)

    # return ctd_num,ctd_df,df
    concat_df['time'] =  pd.to_datetime(concat_df['time_hst'], format='%d-%b-%Y %H:%M:%S') - pd.Timedelta(hours=10)
    concat_df = ip.reorderCol(concat_df,['time_hst','lat','lon','depth','temperature','salinity', 'adjusted_salinity','density','fluorescence','turbidity','oxygen','oxygen_saturation','CTD_num','time'])

    concat_df = concat_df[['time','lat','lon','depth','temperature','salinity', 'adjusted_salinity','density','fluorescence','turbidity','oxygen','oxygen_saturation','CTD_num']]

    concat_df = ip.removeMissings(['time','lat', 'lon','depth'], concat_df)
    #
    concat_df = ip.NaNtoNone(concat_df)
    # return concat_df

    concat_df = ip.colDatatypes(concat_df)
    concat_df = ip.removeDuplicates(concat_df)
    concat_df.to_csv(export_path, index=False)
    ip.sortByTimeLatLonDepth(concat_df, export_path, 'time', 'lat', 'lon', 'depth')
    # return concat_df
    print('export path: ' ,export_path)
    # return concat_df
    return export_path

# ctd_num,ctd_df,df = makeKM1906_Gradients3_uwayCTD(rawFilePath, rawFileName, tableName)
# concat_df = makeKM1906_Gradients3_uwayCTD(rawFilePath, rawFileName, tableName)

export_path = makeKM1906_Gradients3_uwayCTD(rawFilePath, rawFileName, tableName)
iF.toSQLbcp(export_path, tableName,server)
