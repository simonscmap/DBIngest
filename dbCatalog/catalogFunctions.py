
import pycmap
import pandas as pd
import sys
sys.path.append('../')
import dbCore as dc
import catalogInsert as cI
sys.path.append('../login')
import credentials as cr
sys.path.append('../dbCruiseKeywords')
from cruise_keyword_dict import cruise_keyword_dict
import geopandas as gpd
import shapely
import calendar

""" Supporting/catalog table insert functions"""


def insertCruiseKeywords(ID,df,server):
    for keyword in df['cruise_keywords']:
        query = (ID,keyword)
        if len(keyword) > 0: # won't insert empty values
            try: # Cannot insert duplicate entries, so skips if duplicate
                print(query)
                cI.lineInsert(server,'[opedia].[dbo].[tblCruise_Keywords]', '(cruise_ID, keywords)', query)
            except Exception as e:
                pass
                # print(e)


def getCruiseID(cruise_name):
    """input cruise name, output cruise ID"""
    api = pycmap.API()
    df = api.cruise_by_name(cruise_name)
    ID_df = pd.DataFrame({'ID': df['ID']})
    ID = ID_df['ID'].iloc[0]
    return ID

def addDFtoKeywordDF(master_df, df,axis=0):
    df = pd.DataFrame({'cruise_keywords': df.values.flatten()})
    merge_df = pd.concat([master_df,df], ignore_index=True,sort=False)
    return merge_df

def removeAnyRedundantWord(df): # takes dataframe and collapses to list, then rebuilds as single col dataframe
    old_list = df.values.flatten().astype(str)
    new_list = [i.split() for i in old_list]
    newer_list = [item for items in new_list for item in items]
    set_keywords = list(set(newer_list))
    df_set = pd.DataFrame({'cruise_keywords': set_keywords})
    return df_set

def getCruiseDetails(cruise_name):
    """input: cruise name, returns: dataframe of cruise details"""
    api = pycmap.API()
    df = api.cruise_by_name(cruise_name)
    details_df = pd.DataFrame({'Nickname': df['Nickname'],'Name': df['Name'],'Ship_Name': df['Ship_Name'],'Chief_Name': df['Chief_Name']})
    return details_df




def getCruiseYear(cruise_name):
    """input: cruise name, returns: dataframe of year"""
    api = pycmap.API()
    traj = api.cruise_trajectory(cruise_name)
    traj['date'] = pd.to_datetime(traj['time'])
    traj['year'] =  traj['date'].dt.year
    unique_year_df = pd.DataFrame({'Year': traj['year'].unique()})
    unique_year_df['Year'] = unique_year_df['Year'].astype(str).str.strip()
    return unique_year_df


def getCruiseMonths(cruise_name):
    """input: cruise name, returns: dataframe of unique months during cruise"""
    api = pycmap.API()
    traj = api.cruise_trajectory(cruise_name)
    traj['date'] = pd.to_datetime(traj['time'])
    traj['month_abbr'] =  traj['date'].dt.month.apply(lambda x: calendar.month_abbr[x])
    traj['month_name'] =  traj['date'].dt.month.apply(lambda x: calendar.month_name[x])
    unique_months_abbr_df = pd.DataFrame({'Month': traj['month_abbr'].unique()})
    unique_months_name_df = pd.DataFrame({'Month': traj['month_name'].unique()})
    month_df = pd.concat([unique_months_abbr_df,unique_months_name_df],ignore_index=True,sort=False)
    return month_df

def getCruiseSeasons(cruise_name): #gets unique seasons of a cruise. Northern seasons used as a ref. Winter: Dec,Jan,Feb Spring: Mar,Apr,May Summer: June,July,Aug Fall: Sept,Oct,Nov
    """input: cruise name, returns: dataframe of northern hemisphere seasons during cruise"""
    api = pycmap.API()
    traj = api.cruise_trajectory(cruise_name)
    traj['date'] = pd.to_datetime(traj['time'])
    seasons = ['Winter', 'Winter', 'Spring', 'Spring', 'Spring', 'Summer', 'Summer', 'Summer', 'Fall', 'Fall', 'Fall', 'Winter']
    month_to_season = dict(zip(range(1,13), seasons))
    traj['seasons'] = traj['date'].dt.month.map(month_to_season)
    unique_seasons_df = pd.DataFrame({'Season - Northern Hemisphere': traj['seasons'].unique()})
    return unique_seasons_df

"""function gets cruise trajectory and does a spatial join on a longhurst biogeochem province to get the province code, description and prevailing winds
-input cruise name, returns dataframe of unique prov code and desc"""
def getLonghurstProv(cruise_name):
    api = pycmap.API()
    traj = api.cruise_trajectory(cruise_name)
    longhurst_gdf = gpd.read_file('../spatialData/longhurst_v4_2010/longhurst_v4_2010_added_ocean_names.gpkg')
    traj_gdf = gpd.GeoDataFrame(traj.drop(['lat', 'lon'], axis=1),
                                crs={'init': 'epsg:4326'},
                                geometry=[shapely.geometry.Point(yx) for yx in zip(traj.lon, traj.lat)])
    cruise_longhurst = gpd.sjoin(traj_gdf,longhurst_gdf,how='inner',op='intersects')
    prov_df = pd.DataFrame({'ProvCode': cruise_longhurst['ProvCode'].unique(), 'ProvDescr': cruise_longhurst['ProvDescr'].unique()})
    prov_df['Wind'], prov_df['ProvDescr'] = prov_df['ProvDescr'].str.split('-', 1).str
    return prov_df

def getOceanName(cruise_name):
    api = pycmap.API()
    traj = api.cruise_trajectory(cruise_name)
    longhurst_gdf = gpd.read_file('../spatialData/longhurst_v4_2010/longhurst_v4_2010_added_ocean_names.gpkg')
    traj_gdf = gpd.GeoDataFrame(traj.drop(['lat', 'lon'], axis=1),
                                crs={'init': 'epsg:4326'},
                                geometry=[shapely.geometry.Point(yx) for yx in zip(traj.lon, traj.lat)])
    cruise_longhurst = gpd.sjoin(traj_gdf,longhurst_gdf,how='inner',op='intersects')
    ocean_df = pd.DataFrame({'Ocean': cruise_longhurst['Ocean'].unique()})
    ocean_df = ocean_df.dropna()
    return ocean_df


"""input: cruise name, returns: dataframe of cruise variable short name synonyms"""
def getShortNameSynonyms(cruise_name):
    short_name_df = getCruiseAssosiatedShortName(cruise_name)
    lookup_df= short_name_df['Short_Name'].str.lower().map(cruise_keyword_dict)
    lookup_df = lookup_df.dropna().tolist()
    df_look_new = [val for sublist in lookup_df for val in sublist]
    synonyms_df = pd.DataFrame({'synonyms': df_look_new})
    return synonyms_df


"""input: cruise name, returns: dataframe of cruise variable short names"""
def getCruiseAssosiatedShortName(cruise_name):
    api = pycmap.API(token=cr.api_key)
    df = api.cruise_variables(cruise_name)
    cruise_LN = pd.DataFrame({'Short_Name': df['Variable']})
    return cruise_LN

"""input: cruise name, returns: dataframe of cruise variable long names"""
def getCruiseAssosiatedLongName(cruise_name):
    api = pycmap.API(token=cr.api_key)
    df = api.cruise_variables(cruise_name)
    cruise_LN = pd.DataFrame({'Long_Name': df['Long_Name']})
    return cruise_LN


def stripWhitespace(df,col):
    df[col] = df[col].str.strip()
    return df

def removeDuplicates(df):
    df = df.drop_duplicates(keep='first')
    return df

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



def normalize_df(df):
    normalized_df = pd.DataFrame(columns = ['time','lat','lon','depth'],index=['min','max'])
    for val in list(df):
        normalized_df[val[4:]].loc['min'] = df['min_' +val[4:]][0]
        normalized_df[val[4:]].loc['max'] = df['max_' +val[4:]][0]
    return normalized_df
