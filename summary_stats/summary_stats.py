

import sys
sys.path.append('../')
import insertFunctions as iF
sys.path.append('../dbInsert/')
import insertPrep as ip
import dbCore as dc
import config_vault as cfgv
import pandas as pd
import numpy as np
import os
import numpy as np
import glob
pd.options.mode.chained_assignment = None

server = 'Rainier'

def datasetList(server):
    query = """ SELECT DISTINCT Table_Name, Sensor_ID from tblVariables """
    dataset_df = dc.dbRead(query,server)
    return dataset_df
dataset_df = datasetList(server)

def datasetsInStats(server):
    query = """ SELECT DISTINCT Dataset_Name from tblDataset_Stats """
    dataset_df = dc.dbRead(query,server)
    return dataset_df
existing_stats_datasets = datasetsInStats(server)

def updateStatsTable(tableName, JSON):
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    # insertQuery = """UPDATE tblDataset_Stats SET Dataset_Name = '%s', JSON_stats = '%s' """ % (tableName, JSON)
    insertQuery = """INSERT INTO tblDataset_Stats (Dataset_Name, JSON_stats) VALUES('%s','%s')""" % (tableName, JSON)
    # print(insertQuery)
    cursor.execute(insertQuery)
    conn.commit()


def normalize_df(df):
    normalized_df = pd.DataFrame(columns = ['time','lat','lon','depth'],index=['min','max'])
    for val in list(df):
        normalized_df[val[4:]].loc['min'] = df['min_' +val[4:]][0]
        normalized_df[val[4:]].loc['max'] = df['max_' +val[4:]][0]
    return normalized_df

def updateStats(server):
    for tableName, sensor in zip(dataset_df['Table_Name'],dataset_df['Sensor_ID']):
        print(tableName,sensor)
        conn = dc.dbConnect(server)
        if sensor == 2 and tableName != 'tblArgoMerge_REP' and tableName != 'tblWOA_Climatology': # ie, in-situ data
            try:
                query = 'SELECT * FROM %s' % (tableName)
                df = dc.dbRead(query,server)
                stats_df = df.describe()
                min_max_df = pd.DataFrame({'time':[np.min(df['time']),np.max(df['time'])]},index=['min','max'])
                df = pd.concat([stats_df,min_max_df],axis=1, sort=True)
            except:
                print(tableName, ' not inserted')


        elif sensor != '2' or tableName == 'tblArgoMerge_REP' or tableName == 'tblWOA_Climatology': # ie, in-situ data
            try:
                query = 'SELECT min(time) as min_time,max(time) as max_time,min(lat) as min_lat,max(lat) as max_lat,min(lon) as min_lon,max(lon) as max_lon, min(depth) as min_depth,max(depth) as max_depth FROM %s' % (tableName)
                df = dc.dbRead(query,server)
            except:
                try:
                    query = 'SELECT min(time) as min_time,max(time) as max_time,min(lat) as min_lat,max(lat) as max_lat,min(lon) as min_lon,max(lon) as max_lon FROM %s' % (tableName)
                    df = dc.dbRead(query,server)
                except:
                    try:
                        query = 'SELECT min(lat) as min_lat,max(lat) as max_lat,min(lon) as min_lon,max(lon) as max_lon,min(depth) as min_depth,max(depth) as max_depth FROM %s' % (tableName)
                        df = dc.dbRead(query,server)
                    except:
                        try:
                            query = 'SELECT min(lat) as min_lat,max(lat) as max_lat,min(lon) as min_lon,max(lon) as max_lon FROM %s' % (tableName)
                            df = dc.dbRead(query,server)
                        except:
                            print(tableName, ' not inserted')
            try:
                df = normalize_df(df)
            except:
                print(tableName, ' not inserted')

        json_str  = df.to_json(date_format = 'iso')
        sql_df = pd.DataFrame({'Table_Name': [tableName], 'JSON': [json_str]})
        print(sql_df)
        # updateStatsTable(tableName, json_str)
        # except:
        #     print(tableName, ' did not insert into stats table')

        print('\n')
        # input("Press Enter to continue...")

updateStats(server)


def dBtoDF(server,tableName):
    query = """SELECT * FROM tblDataset_Stats WHERE Dataset_Name =  '%s'""" % (tableName)
    df = dc.dbRead(query,server)
    df = pd.read_json(df['JSON_stats'][0])

    return df

# tableName = 'tblDarwin_Ecosystem'
# df = dBtoDF(server,tableName)
#
# df1 = normalize_df(df)
# print('\n')
# print(tableName + ' Summary Statistics: ')
# print('\n','Min lat: ',df['lat'].loc['min'],'Max lat: ',df['lat'].loc['max'],'\n','Min lon: ',df['lon'].loc['min'],'Max lon: ',df['lon'].loc['max'],df['depth'].loc['min'],df['depth'].loc['max'])

# print('\n','Min time: ', df['time'].loc['min'],'Max time: ', df['time'].loc['max'],'\n','Min lat: ',df['lat'].loc['min'],'Max lat: ',df['lat'].loc['max'],'\n','Min lon: ',df['lon'].loc['min'],'Max lon: ',df['lon'].loc['max'],df['depth'].loc['min'],df['depth'].loc['max'])
# """ test with small table - seaflow? lots of vars"""
# # tableName = 'tblAltimetry_NRT'
# # tableName = 'tblAMT13_Chisholm'
# # conn = dc.dbConnect(server)
# # query = 'SELECT * FROM %s' % (tableName)
# query = 'SELECT * FROM %s' % (tableName)
#
# df = dc.dbRead(query,server)
# """ add column for time - merge on min and max index"""
# # min_max_df = pd.DataFrame({'min_time':np.min(df['time']),'max_time':np.max(df['time']),'min_lat':np.min(df['lat']),'max_lat':np.max(df['lat']), 'min_lon':np.min(df['lon']),'max_lon':np.max(df['lon']),'min_depth':np.min(df['depth']),'max_depth':np.max(df['depth'])})
# stats_df = df.describe()
# min_max_df = pd.DataFrame({'time':[np.min(df['time']),np.max(df['time'])]},index=['min','max'])
# concat_df = pd.concat([stats_df,min_max_df],axis=1)
# json_str = stats_df.to_json()
# #
#
#
#
# json_df = pd.read_json(json_str)
# sql_df = pd.DataFrame({'Table_Name': [tableName], 'JSON': [json_str]})
#
# print(json_df.head())
# conn = dc.dbConnect(server)
#
# tableName = 'tblGlobal_PicoPhytoPlankton'
# query = 'SELECT * FROM %s' % (tableName)
# df = dc.dbRead(query,server)
# stats_df = df.describe()
# min_max_df = pd.DataFrame({'time':[np.min(df['time']),np.max(df['time'])]},index=['min','max'])
# df = pd.concat([stats_df,min_max_df],axis=1, sort=True)
