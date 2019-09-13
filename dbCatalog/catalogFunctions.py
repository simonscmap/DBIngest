
import numpy as np
import pandas as pd
import sys
sys.path.append('../')
import dbCore as dc
import catalogInsert as cI
sys.path.append('../login')
import credentials as cr


""" Supporting/catalog table insert functions"""

def lineInsert(tableName, columnList ,query, server):
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    insertQuery = """INSERT INTO %s %s VALUES %s """ % (tableName, columnList, query)
    cursor.execute(insertQuery)
    conn.commit()


def findID(datasetName, catalogTable, server):
    """ this function pulls the ID value from the [tblDatasets] for the tblDataset_References to use """
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str = """select [ID] FROM [Opedia].[dbo].[""" + catalogTable + """] WHERE [Dataset_Name] = '""" + datasetName + """'"""
    cursor.execute(cur_str)
    IDvar = (cursor.fetchone()[0])
    return IDvar

def findVarID(datasetID, Short_Name,  server):
    """ this function pulls the ID value from the [tblVariables] for the tblKeywords to use """
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = '""" + str(datasetID) + """' AND [Short_Name] = '"""+ Short_Name + """'"""
    cursor.execute(cur_str)
    VarID = (cursor.fetchone()[0])
    return VarID

def findMinMaxDate(tableName,server):
    cur_str = 'select min(time), max(time) FROM [Opedia].[dbo].[' + tableName + ']'
    df = dc.dbRead(cur_str,server)
    dates = df.iloc[0].values
    minDate = pd.to_datetime(str(dates[0])).strftime('%Y-%m-%d')
    maxDate = pd.to_datetime(str(dates[1])).strftime('%Y-%m-%d')
    return {'minDate':minDate,
     'maxDate':maxDate}


def findSpatialBounds(tableName,server):
    cur_str = 'select min(lat), max(lat), min(lon), max(lon) FROM [Opedia].[dbo].[' + tableName + ']'
    df = dc.dbRead(cur_str,server)
    dates = df.iloc[0].values
    return {'minLat':dates[0],
     'maxLat':dates[1],  'minLon':dates[2],  'maxLon':dates[3]}


def remove_duplicatesCatalogTables(server):
    catalogTableList = [
     'tblDatasets', 'tblDataset_References', 'tblVariables']
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str_tblDatasets = '\n        WITH list_duplicates (Dataset_Name, Dataset_Long_Name, duplicate_count) AS\n        (\n            SELECT Dataset_Name, Dataset_Long_Name,\n        ROW_NUMBER() OVER(PARTITION BY Dataset_Name, Dataset_Long_Name ORDER BY Dataset_Name, Dataset_Long_Name) AS duplicate_count\n        FROM tblDatasets_copy)\n        DELETE FROM list_duplicates WHERE duplicate_count > 1\n        '
    cur_str_tblDataset_References = '\n        WITH list_duplicates (Dataset_ID, Reference, duplicate_count) AS\n        (\n            SELECT Dataset_ID, Reference,\n        ROW_NUMBER() OVER(PARTITION BY Dataset_ID, Reference ORDER BY Dataset_ID, Reference) AS duplicate_count\n        FROM tblDataset_References_copy)\n        DELETE FROM list_duplicates WHERE duplicate_count > 1\n        '
    cur_str_tblVariables = '\n        WITH list_duplicates (Table_Name, Short_Name, Long_Name, duplicate_count) AS\n        (\n            SELECT Table_Name, Short_Name, Long_Name,\n        ROW_NUMBER() OVER(PARTITION BY Table_Name, Short_Name, Long_Name ORDER BY Table_Name, Short_Name, Long_Name) AS duplicate_count\n        FROM tblVariables_copy)\n        DELETE FROM list_duplicates WHERE duplicate_count > 1\n        '
    cursor.execute(tblDatasets)
    cursor.execute(tblDataset_References)
    cursor.execute(tblVariables)

def findVariables(datasetName, catalogTable,server):
    conn = dc.dbConnect()
    cursor = conn.cursor()
    cur_str = """select [Variables] FROM [Opedia].[dbo].[""" + catalogTable + """] WHERE [Dataset_Name] = '""" + datasetName + """'"""
    cursor.execute(cur_str)
    IDvar = (cursor.fetchone()[0])
    varlist = IDvar.split(',')
    return varlist

def deleteCatalogTables(datasetName,server):
    contYN = input('Are you sure you want to delete all of the catalog tables for ' + datasetName + ' ?  [yes/no]: ' )
    if contYN == 'yes':
        """ delete tblVariables, then tblDataset_References then finally tblDatasets """
        print('connecting to database...')
        conn = dc.dbConnect()
        cursor = conn.cursor()
        print('db connection successful')

        Dataset_ID = str(findID(datasetName, catalogTable = 'tblDatasets')) #datasetName
        print('Dataset ID used to remove catalog tables: ', Dataset_ID)

        cur_str = """DELETE FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = """ + Dataset_ID
        cursor.execute(cur_str)
        print('-- Instances of ' + datasetName + ' removed from tblVariables')

        cur_str = """DELETE FROM [Opedia].[dbo].[tblDataset_References] WHERE [Dataset_ID] = """ + Dataset_ID
        cursor.execute(cur_str)
        print('-- Instances of ' + datasetName + ' removed from tblDataset_References')

        cur_str = """DELETE FROM [Opedia].[dbo].[tblDatasets] WHERE [ID] = """ + Dataset_ID
        cursor.execute(cur_str)
        print('-- Instances of ' + datasetName + ' removed from tblDatasets')
        print('Commiting changes...')
        conn.commit()
        conn.rollback()
        print('Changes to dB commited')
    else:
        print('Catalog tables for ' + datasetName + ' not deleted')

def tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server):
    """ create a tuple out of variables and columns -- in future edit Climatology should be part of insert prep function to reduce repitition """
    if Climatology == 'NULL':
        query = (DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description)
        columnList = '(DB,Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description)'
        print('Inserting data into tblDatasets')
        # print(server, '[opedia].[dbo].[tblDatasets]', columnList, query)
        cI.lineInsert(server,'[opedia].[dbo].[tblDatasets]', columnList, query)

    else:
        if Climatology == '1':
            columnList = '(ID, DB,Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology)'
            query = (DB_id,DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology)
        print('Inserting data into tblDatasets')
        cI.lineInsert(server,'[opedia].[dbo].[tblDatasets]', columnList, query)

def tblDataset_References(Dataset_Name, reference_list,server):
    IDvar = findID(Dataset_Name, 'tblDatasets',server)
    columnList = '(Dataset_ID, Reference)'
    for ref in reference_list:
        query = (IDvar, ref)
        cI.lineInsert(server,'[opedia].[dbo].[tblDataset_References]', columnList, query)
    print('Inserting data into tblDataset_References')

def tblVariables(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list,Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server):
    Dataset_ID_raw = cI.findID(Dataset_Name_list[0], 'tblDatasets', server)
    dataset_ID_list = [Dataset_ID_raw] * len(DB_list)
    columnList = '(DB, Dataset_ID, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID, Spatial_Res_ID, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID, Comment)'
    for DB, dataset_ID, Dataset_Name, short_name, long_name, unit, temporal_res, spatial_res, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID,  comment in zip(DB_list, dataset_ID_list, Dataset_Name_list, short_name_list, long_name_list, unit_list, temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list,  comment_list):
        query = (DB, dataset_ID, Dataset_Name, short_name, long_name, unit, temporal_res, spatial_res, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID,  comment)
        cI.lineInsert(server,'[opedia].[dbo].[tblVariables]', columnList, query)
    print('Inserting data into tblVariables')

def tblKeywords(df,Dataset_Name, keyword_col,tableName,server):
    IDvar = findID(Dataset_Name, 'tblDatasets',server)
    for index,row in df.iterrows():
        VarID = findVarID(IDvar, df.loc[index,'var_short_name'],  server)
        # keyword_list = df.loc[index,keyword_col]
        keyword_list = (df.loc[index,keyword_col]).split(',')

        # # print(keyword_list)
        for keyword in keyword_list:
            keyword = keyword.lstrip()
            print(VarID, keyword)
            query = (VarID, keyword)
            if len(keyword) > 0: # won't insert empty values
                try: # Cannot insert duplicate entries, so skips if duplicate
                    cI.lineInsert(server,'[opedia].[dbo].[tblKeywords]', '(var_ID, keywords)', query)
                except Exception as e:
                    print(e)





# def tblDataset_References(Dataset_Name, reference_list,server):
#     IDvar = findID(Dataset_Name, 'tblDatasets',server)
#     columnList = '(Dataset_ID, Reference)'
#     for ref in reference_list:
#         query = (IDvar, ref)
#         print(query)
#         # cI.lineInsert(server,'[opedia].[dbo].[tblDataset_References]', columnList, query)
#     print('Inserting data into tblDataset_References')

def updateStatsTable(tableName, JSON):
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    insertQuery = """INSERT INTO tblDataset_Stats (Dataset_Name, JSON_stats) VALUES('%s','%s')""" % (tableName, JSON)
    cursor.execute(insertQuery)
    conn.commit()

def normalize_df(df):
    normalized_df = pd.DataFrame(columns = ['time','lat','lon','depth'],index=['min','max'])
    for val in list(df):
        normalized_df[val[4:]].loc['min'] = df['min_' +val[4:]][0]
        normalized_df[val[4:]].loc['max'] = df['max_' +val[4:]][0]
    return normalized_df

def updateStats(tableName, server):
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
    updateStatsTable(tableName, json_str)
