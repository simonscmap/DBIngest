import sys
sys.path.append('../../config/')
import config_vault as cfgv
sys.path.append('../')
import insertFunctions as iF
sys.path.append('../dbCatalog/')
import catalogFunctions as cF
sys.path.append('../dbInsert/')
import insertPrep as ip
import dbCore as dc
import pandas as pd
import numpy as np
import os
import numpy as np
import glob
pd.options.mode.chained_assignment = None
import pycmap
import dask.dataframe as dd
import datetime


def dBtoDF(tableName,server):
    query = """SELECT * FROM tblDataset_Stats WHERE Dataset_Name =  '%s'""" % (tableName)
    print(query)
    df = dc.dbRead(query,server)
    df = pd.read_json(df['JSON_stats'][0])
    return df

def deletefromStatsTable(tableName,server):
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    insertQuery = """DELETE FROM tblDataset_Stats where Dataset_Name = '%s'""" % (tableName)
    cursor.execute(insertQuery)
    conn.commit()

def updateStatsTable(tableName, json_str, server):
    print('Updating stats for: ', tableName)
    Dataset_ID = cF.findDatasetID(tableName, server)
    try:
        conn = dc.dbConnect(server)
        cursor = conn.cursor()
        insertQuery = """INSERT INTO tblDataset_Stats (Dataset_Name, JSON_stats) VALUES('%s','%s')""" % (tableName, json_str)
        # insertQuery = """INSERT INTO tblDataset_Stats_ID (Dataset_ID, JSON_stats) VALUES('%s','%s')""" % (Dataset_ID, json_str)
        cursor.execute(insertQuery)
        conn.commit()
    except Exception as e:
        print(e)

def normalize_df(df):
    normalized_df = pd.DataFrame(columns = ['time','lat','lon','depth'],index=['min','max'])
    for val in list(df):
        normalized_df[val[4:]].loc['min'] = df['min_' +val[4:]][0]
        normalized_df[val[4:]].loc['max'] = df['max_' +val[4:]][0]
    return normalized_df

def query_df(query,server):
    df = dc.dbRead(query,server)
    single_val= df.iloc[0]
    return single_val

def getSensor(tableName,server):
    query = """ SELECT Sensor_ID from tblVariables where Table_Name = '%s'""" % (tableName)
    sensor_df = dc.dbRead(query,server)
    sensor = sensor_df['Sensor_ID'].iloc[0]
    return sensor

def getLatCount(tableName):
    query = """SELECT SUM(p.rows) FROM sys.partitions AS p
    INNER JOIN sys.tables AS t
    ON p.[object_id] = t.[object_id]
    INNER JOIN sys.schemas AS s
    ON s.[schema_id] = t.[schema_id]
    WHERE t.name = N'""" + tableName + """'
    AND s.name = N'dbo'
    AND p.index_id IN (0,1);"""

    api = pycmap.API()
    df = api.query(query)
    lat_count = df.columns[0]
    return lat_count

def satOrModel_ST(tableName,var_name,server):
    var_min = query_df(""" SELECT min("""+var_name+""") from """+tableName,server).iloc[0]
    var_max = query_df(""" SELECT max("""+var_name+""") from """+tableName,server).iloc[0]
    return var_min,var_max

def satOrModel_stats_agg(tableName,var_name,interval = 'y'):
    api = pycmap.API()
    if api.is_climatology(tableName) == True:
        df = api.space_time(table=tableName,variable=var_name,dt1='1900-01-01',dt2='2030-12-31',lat1=-90,lat2=90,lon1=-180,lon2=180,depth1=0,depth2=10000)
    else:
        df = api.time_series(table=tableName,variable=var_name,dt1='1900-01-01',dt2='2030-12-31',lat1=-90,lat2=90,lon1=-180,lon2=180,depth1=0,depth2=10000,interval=interval)
    var_min = min(df[var_name])
    var_max = max(df[var_name])
    var_count = len(df[var_name])
    var_mean = np.mean(df[var_name])
    var_std = np.std(df[var_name])
    return var_min,var_max,var_count,var_mean,var_std


def satOrModel_stats(tableName,var_name,server):
    var_min = query_df(""" SELECT min("""+var_name+""") from """+tableName,server).iloc[0]
    var_max = query_df(""" SELECT max("""+var_name+""") from """+tableName,server).iloc[0]
    var_count = query_df(""" SELECT COUNT_BIG("""+var_name+""") from """+tableName,server).iloc[0]
    var_mean = query_df(""" SELECT Avg("""+var_name+""") from """+tableName,server).iloc[0]
    var_std = query_df(""" SELECT STDEV("""+var_name+""") from """+tableName,server).iloc[0]
    return var_min,var_max,var_count,var_mean,var_std


def getTableVars(tableName, server):
    query = """SELECT Short_Name FROM [Opedia].[dbo].[tblVariables] WHERE Table_Name =  '%s'""" % (tableName)
    df = dc.dbRead(query,server)
    return df

def insertVarDFLargeTables(tableName, df,server):
    json_str  = df.to_json(date_format = 'iso')
    sql_df = pd.DataFrame({'Table_Name': [tableName], 'JSON': [json_str]})
    deletefromStatsTable(tableName,server)
    updateStatsTable(tableName, json_str,server)

def aggregate_daily_stats_coords(tableName, ST_param):
    flist = glob.glob(cfgv.dataset_stats_raw + tableName + '/' + '*.csv*')
    df = dd.read_csv(cfgv.dataset_stats_raw + tableName + '/' + '*.csv*')
    df = df.compute().set_index(df.columns[0])
    df.index.name = 'Stats'
    if ST_param == 'time':
        var_min = os.path.basename(min(flist)).split('_')[0]
        var_max = os.path.basename(max(flist)).split('_')[0]
    else:
        var_min = min(df.loc['min', ST_param])
        var_max = max(df.loc['max', ST_param])

    return var_min, var_max

def aggregate_daily_stats_vars(tableName, var):
    df = dd.read_csv(cfgv.dataset_stats_raw + tableName + '/' + '*.csv*')
    df = df.compute().set_index(df.columns[0])
    df.index.name = 'Stats'
    max_var_count = getLatCount(tableName)
    var_max = max(df.loc['max', var])
    var_min = min(df.loc['min', var])
    var_mean = np.mean(df.loc['mean', var])
    var_std = np.std(df.loc['std', var])
    return max_var_count, var_max, var_min, var_mean, var_std


def aggregate_daily_stats_vars_list(tableName, var_list):
    df = dd.read_csv(cfgv.dataset_stats_raw + tableName + '/' + '*.csv*')
    df = df.compute().set_index(df.columns[0])
    df.index.name = 'Stats'
    var_max_list, var_min_list, var_mean_list, var_std_list = [],[],[],[]
    max_var_count_list = getLatCount(tableName) * len(var_list)
    for var in var_list:
        var_max = max(df.loc['max', var])
        var_max_list.append(var_max)
        var_min = min(df.loc['min', var])
        var_min_list.append(var_min)
        var_mean = np.mean(df.loc['mean', var])
        var_mean_list.append(var_mean)
        var_std = np.std(df.loc['std', var])
        var_std_list.append(var_std)

    return max_var_count_list, var_max_list, var_min_list, var_mean_list, var_std_list

def buildVarDFLargeTables(tableName,server): ## This function builds a stats tables without taking tables into memory using sql calls. It does not compute quantiles
    var_names = getTableVars(tableName,server)
    # return var_names
    stats_DF = pd.DataFrame(index=['count','max','mean','min','std'])
    api = pycmap.API()
    climYN = api.is_climatology(tableName)
    hasDepth = api.has_field(tableName,'depth')
    print('Building stats for :', tableName)
    print("Climatology is: ", climYN)
    print("Depth is: " , hasDepth)
    """ gets stats on time/lat/lon/depth"""

    if climYN == False and hasDepth == True:         # has time/lat/lon/depth
        ST_list = ['time','lat','lon','depth']
    elif climYN == False and hasDepth == False:         # has time/lat/lon/
        ST_list = ['time','lat','lon']
    elif climYN == True and hasDepth == True:         # has lat/lon/depth
        ST_list = ['lat','lon','depth']
    elif climYN == True and hasDepth == False:         # has lat/lon
        ST_list = ['lat','lon']

    for ST_val in ST_list:
        try:
            var_min,var_max = aggregate_daily_stats_coords(tableName, ST_val)
            stats_DF[ST_val] = '' # create empty column to fill
            stats_DF.at['min', ST_val] = var_min
            stats_DF.at['max', ST_val] = var_max
        #
        except Exception as e:
            print(e)

    stats_DF.at['count', 'lat'] = getLatCount(tableName)
    """ gets stats on variables """

    # try:
    var_list = list(var_names['Short_Name'])
    max_var_count_list, var_max_list, var_min_list, var_mean_list, var_std_list = aggregate_daily_stats_vars_list(tableName, var_list)
    for var, max_var_count, var_max, var_min, var_mean, var_std  in zip(var_list,max_var_count_list, var_max_list, var_min_list, var_mean_list, var_std_list):
        print(var, max_var_count, var_max, var_min, var_mean, var_std)
        stats_DF[var] = '' # create empty column to fill
        stats_DF.at['count', var] = max_var_count
        stats_DF.at['max', var] = var_max
        stats_DF.at['mean', var] = var_mean
        stats_DF.at['min', var] = var_min
        stats_DF.at['std', var] = var_std

    return stats_DF



def buildVarDFSmallTables(tableName,server): #Builds stats table entry for small tables (ex cruise)
        try:
            query = 'SELECT * FROM %s' % (tableName)
            df = dc.dbRead(query,server)
            stats_df = df.describe()
            min_max_df = pd.DataFrame({'time':[np.min(df['time']),np.max(df['time'])]},index=['min','max'])
            df = pd.concat([stats_df,min_max_df],axis=1, sort=True)
            json_str  = df.to_json(date_format = 'iso')
            sql_df = pd.DataFrame({'Table_Name': [tableName], 'JSON': [json_str]})
            updateStatsTable(tableName, json_str,server)
        except Exception as e:
            print(e)





# buildVarDFSmallTables('tblAloha_Deep_Trap_Omics','Rainier')
# buildVarDFSmallTables('tblAMT13_Chisholm','Rainier')
# # buildVarDFSmallTables('tblArgoMerge_REP','Rainier')
# buildVarDFSmallTables('tblBottle_Chisholm','Rainier')
# buildVarDFSmallTables('tblCruise_PAR','Rainier')
# buildVarDFSmallTables('tblCruise_Salinity','Rainier')
# buildVarDFSmallTables('tblCruise_Temperature','Rainier')
# buildVarDFSmallTables('tblCTD_Chisholm','Rainier')
# buildVarDFSmallTables('tblDeLong_HOT_metagenomics','Rainier')
# buildVarDFSmallTables('tblESV','Rainier')
# buildVarDFSmallTables('tblFalkor_2018','Rainier')
# buildVarDFSmallTables('tblFlombaum','Rainier')
# buildVarDFSmallTables('tblGlobal_PicoPhytoPlankton','Rainier')
# buildVarDFSmallTables('tblGlobalDrifterProgram','Rainier')
# buildVarDFSmallTables('tblGLODAP','Rainier')
# buildVarDFSmallTables('tblHL2A_diel_metagenomics','Rainier')
# buildVarDFSmallTables('tblHOE_legacy_2A_Caron_Omics','Rainier')
# buildVarDFSmallTables('tblHOE_legacy_3_Caron_Omics','Rainier')
# buildVarDFSmallTables('tblHOT_Bottle','Rainier')
# buildVarDFSmallTables('tblHOT_CTD','Rainier')
# buildVarDFSmallTables('tblHOT_EpiMicroscopy','Rainier')
# buildVarDFSmallTables('tblHOT_LAVA','Rainier')
# buildVarDFSmallTables('tblHOT_Macrozooplankton','Rainier')
# buildVarDFSmallTables('tblHOT_ParticleFlux','Rainier')
# buildVarDFSmallTables('tblHOT_PP','Rainier')
# buildVarDFSmallTables('tblHOT273_Caron_Omics','Rainier')
# buildVarDFSmallTables('tblKM1314_Cobalmins','Rainier')
# # buildVarDFSmallTables('tblKM1314_ParticulateCobalamins','Rainier')
# buildVarDFSmallTables('tblKM1513_HOE_legacy_2A_Dyhrman_Omics','Rainier')
# buildVarDFSmallTables('tblKM1605_HOE_Legacy_3','Rainier')
# buildVarDFSmallTables('tblKM1709_mesoscope','Rainier')
# buildVarDFSmallTables('tblKOK1606_Gradients1_TargetedMetabolites','Rainier')
# buildVarDFSmallTables('tblKM1709_mesoscope_Dyhrman_Omics','Rainier')
# buildVarDFSmallTables('tblKM1906_Gradients3','Rainier')
# buildVarDFSmallTables('tblKM1906_Gradients3_uw_tsg','Rainier')
# buildVarDFSmallTables('tblKM1906_Gradients3_uwayCTD','Rainier')
# buildVarDFSmallTables('tblKOK1507_HOE_Legacy_2B','Rainier')
# buildVarDFSmallTables('tblKOK1606_Gradients1_Cobalamins','Rainier')
# buildVarDFSmallTables('tblKOK1606_Gradients1_Diazotroph','Rainier')
# buildVarDFSmallTables('tblKM1709_mesoscope_CTD','Rainier')
# buildVarDFSmallTables('tblKOK1606_Gradients1_uway_optics','Rainier')
# buildVarDFSmallTables('tblKOK1607_HOE_Legacy_4','Rainier')
# buildVarDFSmallTables('tblKOK1607_HOE_legacy_4_Dyhrman_Omics','Rainier')
# buildVarDFSmallTables('tblKOK1806_HOT_LAVA_Dyhrman_Omics','Rainier')
# buildVarDFSmallTables('tblMGL1704_Gradients2_Cobalamins','Rainier')
# buildVarDFSmallTables('tblMGL1704_Gradients2_Diazotroph','Rainier')
# buildVarDFSmallTables('tblMGL1704_Gradients2_TargetedMetabolites','Rainier')
# buildVarDFSmallTables('tblMGL1704_Gradients2_uway_optics','Rainier')
# buildVarDFSmallTables('tblSCOPE_HOT_metagenomics','Rainier')
# buildVarDFSmallTables('tblSeaFlow','Rainier')
# buildVarDFSmallTables('tblSingleCellGenomes_Chisholm','Rainier')
# buildVarDFSmallTables('tblWOA_Climatology','Rainier')



# buildVarDFLargeTables('tblSST_AVHRR_OI_NRT','Rainier', 'm')
# buildVarDFLargeTables('tblArgoMerge_REP','Rainier', 'm')
# buildVarDFLargeTables('tblCHL_REP','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Chl_Climatology','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Ecosystem','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Nutrient','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Nutrient_Climatology','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Ocean_Color','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Phytoplankton','Rainier', 'm')
# buildVarDFLargeTables('tblDarwin_Plankton_Climatology','Rainier', 'm')
# buildVarDFLargeTables('tblMercator_MLD_NRT','Rainier', 'm')
# buildVarDFLargeTables('tblMesoscale_Eddy','Rainier','y')
# buildVarDFLargeTables('tblModis_AOD_REP','Rainier', 'm')
# buildVarDFLargeTables('tblPisces_NRT','Rainier', 'm')
# buildVarDFLargeTables('tblSSS_NRT','Rainier', 'm')
# buildVarDFLargeTables('tblSST_AVHRR_OI_NRT','Rainier', 'm')
# buildVarDFLargeTables('tblWind_NRT','Rainier', 'm')
# buildVarDFLargeTables('tblWOA_Climatology','Rainier', 'm')


"""
For large table stats:
two funcs -
def build_daily_stats(tableName, startDate, endDate):
    This func will use pycmap to subset by day and then df.describe dataset into stats,
    then save to csv or sql table

def build summary_stats_of_daily_stats(tableName? directory name?):
    import all csv or all of table from dataset then make stats of stats, writes to db

"""


# var_count, var_max, var_min, var_mean, var_std =  aggregate_daily_stats('tblSST_AVHRR_OI_NRT')


def dataframe_describe_write(df, tableName):
    df_describe = df.describe()
    cfgv.makedir(cfgv.dataset_stats_raw + tableName)
    date = datetime.datetime.strptime(df['time'].iloc[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    df_describe.to_csv(cfgv.dataset_stats_raw + tableName + '/' + date + '_' + tableName + '.csv', sep = ',')
    print('\n')

def build_daily_stats(tableName, startDate, endDate,freq): #'tblSST_AVHRR_OI_NRT', '1990-01-01', '1990-01-02'
    api = pycmap.API()
    climYN = api.is_climatology(tableName)
    if climYN == True:
        datelist = pd.date_range('2010-01-01','2010-12-01',freq=freq).strftime('%Y-%m-%d %H:%M:%S').tolist()
    else:
        datelist = pd.date_range(startDate, endDate,freq=freq).strftime('%Y-%m-%d %H:%M:%S').tolist()

    for date in datelist:
        print('Creating Summary Stats for ' + tableName + ' for ' + date + ' at time: ' + str(datetime.datetime.now()))
        try:
            df = api.space_time(table=tableName, variable='*', dt1=date, dt2=date, lat1=-90, lat2=90, lon1=-180, lon2=180, depth1=0, depth2=10000)
            dataframe_describe_write(df, tableName)
        except:
            print('No data found for date ' + date)







### Idea use pycmap stats to query recent min,max dates of table ###
# build_daily_stats('tblSST_AVHRR_OI_NRT', '2008-01-12', '2019-04-27',freq='D')
# build_daily_stats('tblArgoMerge_REP', '2002-09-08', '2018-06-07',freq='D')
# build_daily_stats('tblCHL_REP', '1998-01-01', '2018-06-26',freq='D')
# build_daily_stats('tblDarwin_Chl_Climatology', '2010-01-01','2010-12-01',freq='MS')
# build_daily_stats('tblDarwin_Ecosystem', '2001-05-02', '2015-12-30',freq='D')
# build_daily_stats('tblMODIS_PAR', '2003-01-01', '2018-12-19',freq='D')
# build_daily_stats('tblModis_AOD_REP', '2002-07-01', '2019-02-01',freq='D')
# build_daily_stats('tblMesoscale_Eddy','1993-01-01','2018-01-18',freq='D')
# build_daily_stats('tblSSS_NRT', '2015-03-31', '	2019-04-21',freq='D')
# build_daily_stats('tblWOA_Climatology','2010-01-01','2010-12-01',freq='MS')
# build_daily_stats('tblMercator_MLD_NRT', '2019-01-01', '2019-04-28',freq='D')
# build_daily_stats('tblWind_NRT', '2015-03-27', '2017-12-04',freq='5H')

# build_daily_stats('tblDarwin_Nutrient', '2013-04-20', '2015-12-30',freq='D')
# build_daily_stats('tblDarwin_Ecosystem', '2006-01-12', '2015-12-30',freq='D')

# build_daily_stats('tblDarwin_Ocean_Color', '2009-12-04', '2015-12-30',freq='D')
# build_daily_stats('tblDarwin_Phytoplankton', '1993-12-31', '2015-12-30',freq='D')

# build_daily_stats('tblDarwin_Nutrient_Climatology', '2010-01-01','2010-12-01',freq='MS')
# build_daily_stats('tblDarwin_Ocean_Color', '1993-12-31', '2015-12-30',freq='D')
# build_daily_stats('tblDarwin_Plankton_Climatology', '2010-01-01','2010-12-01',freq='MS')
# build_daily_stats('tblMercator_MLD_NRT', '2019-01-01', '2019-04-28',freq='D')
# build_daily_stats('tblPisces_NRT', '2011-12-31', '2019-04-27',freq='D')
# build_daily_stats('tblNOAA_NSIDC_CDR_Sea_Ice', '1978-11-01', '2018-12-31',freq='D')


#
# df_tblSST_AVHRR_OI_NRT = getLatCount('tblSST_AVHRR_OI_NRT')
# df_tblArgoMerge_REP = getLatCount('tblArgoMerge_REP')
# df_tblCHL_REP = getLatCount('tblCHL_REP')
# df_tblDarwin_Chl_Climatology = getLatCount('tblDarwin_Chl_Climatology')
# df_tblDarwin_Ecosystem = getLatCount('tblDarwin_Ecosystem')
# df_tblDarwin_Nutrient = getLatCount('tblDarwin_Nutrient')
# df_tblDarwin_Nutrient_Climatology = getLatCount('tblDarwin_Nutrient_Climatology')
# df_tblDarwin_Ocean_Color = getLatCount('tblDarwin_Ocean_Color')
# df_tblDarwin_Phytoplankton = getLatCount('tblDarwin_Phytoplankton')
# df_tblDarwin_Plankton_Climatology = getLatCount('tblDarwin_Plankton_Climatology')
# df_tblMercator_MLD_NRT = getLatCount('tblMercator_MLD_NRT')
# df_tblMesoscale_Eddy = getLatCount('tblMesoscale_Eddy')
# df_tblModis_AOD_REP = getLatCount('tblModis_AOD_REP')
# df_tblPisces_NRT = getLatCount('tblPisces_NRT')
# df_tblSSS_NRT = getLatCount('tblSSS_NRT')
# df_tblWind_NRT = getLatCount('tblWind_NRT')
# df_tblWOA_Climatology = getLatCount('tblWOA_Climatology')
