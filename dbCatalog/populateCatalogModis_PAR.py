import sys
sys.path.append('../')
import insertFunctions as iF
import config_vault as cfgv
import pandas as pd
import catalogFunctions as cF
sys.path.append('../dbInsert/')
import insertPrep as ip
sys.path.append('../summary_stats/')
# import summary_stats_func as ssf

tableName = 'tblModis_PAR'
rawFilePath = cfgv.doc_MODIS_PAR_daily_raw
rawFileName = 'Modis_PAR_metadata.xlsx'
server = 'Rainier'
keyword_col = 'var_keywords'
############################





dataset_metadata = pd.read_excel(rawFilePath + rawFileName, sheet_name = 0)
vars_metadata = pd.read_excel(rawFilePath + rawFileName,sheet_name = 1)


""" Strings """
DB='Opedia'
Dataset_Name = dataset_metadata.iloc[0]['dataset_short_name']
Dataset_Long_Name = dataset_metadata.iloc[0]['dataset_long_name']
Data_Source = dataset_metadata.iloc[0]['dataset_source']

Distributor = 'Ocean Biology Processing Group at NASA GSFC'

Description = dataset_metadata.iloc[0]['dataset_description']
Climatology = 'NULL'
Variables =  ', '.join(list(vars_metadata['var_short_name']))
reference_list = list(dataset_metadata['dataset_references'])
#
# #
# #
DB_list = [DB] * len(vars_metadata)
server_list = [server] * len(vars_metadata)
Dataset_Name_list = [Dataset_Name] * len(vars_metadata)
Table_Name_list = [tableName] * len(vars_metadata)
short_name_list = list(vars_metadata['var_short_name'])
long_name_list = list(vars_metadata['var_long_name'])
unit_list = list(ip.NaNtoNone(vars_metadata['var_unit']))

spatial_res_list = list(ip.NaNtoNone(vars_metadata['var_spatial_res']))
temporal_res_list = list('3') * len(vars_metadata) # Daily

comment_list = list(ip.NaNtoNone(vars_metadata['var_comment']))
Temporal_Coverage_Begin_list = [cF.findMinMaxDate(tableName,server)['minDate']]  * len(vars_metadata)
Temporal_Coverage_End_list = [cF.findMinMaxDate(tableName,server)['maxDate']] * len(vars_metadata)
Lat_Coverage_Begin_list = [cF.findSpatialBounds(tableName,server)['minLat']] * len(vars_metadata)
Lat_Coverage_End_list = [cF.findSpatialBounds(tableName,server)['maxLat']] * len(vars_metadata)
Lon_Coverage_Begin_list = [cF.findSpatialBounds(tableName,server)['minLon']] * len(vars_metadata)
Lon_Coverage_End_list = [cF.findSpatialBounds(tableName,server)['maxLon']] * len(vars_metadata)
Grid_Mapping_list = ['CRS']  * len(vars_metadata)
Make_ID_list = ['1'] * len(vars_metadata)#Observation
Sensor_ID_list = ['1'] * len(vars_metadata) # satellite
Process_ID_list = ['2'] * len(vars_metadata) # Reprocessed
Study_Domain_ID_list = ['1'] * len(vars_metadata) # Biology

# print(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list,Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list)
# print(len(DB_list),len(Dataset_Name_list),len(short_name_list),len(long_name_list),len(unit_list),len(temporal_res_list),len(spatial_res_list),len(Temporal_Coverage_Begin_list),len(Temporal_Coverage_End_list),len(Lat_Coverage_Begin_list),len(Lat_Coverage_End_list),len(Lon_Coverage_Begin_list),len(Lon_Coverage_End_list),len(Grid_Mapping_list),len(Make_ID_list),len(Sensor_ID_list),len(Process_ID_list),len(Study_Domain_ID_list),len(comment_list))
# print('DB: ', DB, '\n', '\n', 'Dataset_Name: ', Dataset_Name, '\n', '\n', 'Dataset_Long_Name: ',Dataset_Long_Name, '\n', '\n', 'Variables: ',Variables, '\n', '\n', 'Data_Source: ', Data_Source, '\n', '\n', 'Distributor: ', Distributor, '\n', '\n', 'Description: ', Description, '\n', '\n', 'Climatology: ',Climatology)

# print(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list,Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, keyword_list, comment_list)
# print(len(DB_list),len(Dataset_Name_list),len(short_name_list),len(long_name_list),len(unit_list),len(temporal_res_list),len(spatial_res_list),len(Temporal_Coverage_Begin_list),len(Temporal_Coverage_End_list),len(Lat_Coverage_Begin_list),len(Lat_Coverage_End_list),len(Lon_Coverage_Begin_list),len(Lon_Coverage_End_list),len(Grid_Mapping_list),len(Make_ID_list),len(Sensor_ID_list),len(Process_ID_list),len(Study_Domain_ID_list),len(comment_list))
# print(DB_list, '\n', '\n', Dataset_Name_list, '\n', '\n', short_name_list, '\n', '\n', long_name_list, '\n', '\n', unit_list, '\n', '\n',temporal_res_list, '\n', '\n', spatial_res_list, '\n', '\n', Temporal_Coverage_Begin_list, '\n', '\n', Temporal_Coverage_End_list, '\n', '\n', Lat_Coverage_Begin_list, '\n', '\n', Lat_Coverage_End_list, '\n', '\n', Lon_Coverage_Begin_list, '\n', '\n', Lon_Coverage_End_list, '\n', '\n', Grid_Mapping_list, '\n', '\n',Make_ID_list, '\n', '\n', Sensor_ID_list, '\n', '\n', Process_ID_list, '\n', '\n', Study_Domain_ID_list, '\n', '\n',  comment_list, '\n', '\n', server)

cF.tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server)
cF.tblDataset_References(Dataset_Name, reference_list,server)
cF.tblVariables(DB_list, Dataset_Name_list, Table_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list,Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server)
cF.tblKeywords(vars_metadata, Dataset_Name,keyword_col,tableName,server)
