import sys
sys.path.append('../')
import insertFunctions as iF
import config_vault as cfgv
import pandas as pd
import catalogFunctions as cF
sys.path.append('../dbInsert/')
import insertPrep as ip
sys.path.append('../summary_stats/')
import summary_stats_func as ssf

"""Cruise_Temperature data Catalog Table"""
server = 'Rainier'
tableName = 'tblCruise_Temperature'
rawFilePath = cfgv.rep_Cruise_Temperature_raw
rawFileName = 'cruise_temperature_metadata.xlsx'
keyword_col = 'var_keywords'

############################



dataset_metadata = pd.read_excel(rawFilePath + rawFileName, sheet_name = 0)
dataset_metadata = ip.removeLeadingWhiteSpace(dataset_metadata)
vars_metadata = pd.read_excel(rawFilePath + rawFileName,sheet_name = 1)


""" Strings """
DB='Opedia'
Dataset_Name = dataset_metadata.iloc[0]['dataset_short_name']
Dataset_Long_Name = dataset_metadata.iloc[0]['dataset_long_name']
Data_Source = dataset_metadata.iloc[0]['dataset_source']

Distributor = 'Multiple Distributors'

Description = dataset_metadata.iloc[0]['dataset_description']
Climatology = 'NULL'
Variables =  ', '.join(list(vars_metadata['var_short_name']))
reference_list = (dataset_metadata.iloc[0]['dataset_references']).split(";")
#
#
DB_list = [DB] * len(vars_metadata)
server_list = [server] * len(vars_metadata)
Dataset_Name_list = [tableName] * len(vars_metadata)
short_name_list = list(vars_metadata['var_short_name'])
long_name_list = list(vars_metadata['var_long_name'])
unit_list = list(ip.NaNtoNone(vars_metadata['var_unit']))

spatial_res_list = list('1') * len(vars_metadata)# Irregular
temporal_res_list = list('7') * len(vars_metadata) # Irregular

comment_list = list(ip.NaNtoNone(vars_metadata['var_comment']))
Temporal_Coverage_Begin_list = [cF.findMinMaxDate(tableName,server)['minDate']]  * len(vars_metadata)
Temporal_Coverage_End_list = [cF.findMinMaxDate(tableName,server)['maxDate']] * len(vars_metadata)
Lat_Coverage_Begin_list = [cF.findSpatialBounds(tableName,server)['minLat']] * len(vars_metadata)
Lat_Coverage_End_list = [cF.findSpatialBounds(tableName,server)['maxLat']] * len(vars_metadata)
Lon_Coverage_Begin_list = [cF.findSpatialBounds(tableName,server)['minLon']] * len(vars_metadata)
Lon_Coverage_End_list = [cF.findSpatialBounds(tableName,server)['maxLon']] * len(vars_metadata)
Grid_Mapping_list = ['CRS']  * len(vars_metadata)
Make_ID_list = ['1'] * len(vars_metadata)#Observation
Sensor_ID_list = ['2'] * len(vars_metadata) # In-Situ
Process_ID_list = ['2'] * len(vars_metadata) # Reprocessed
Study_Domain_ID_list = ['1'] * len(vars_metadata)

# print(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list,Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list)
# print(len(DB_list),len(Dataset_Name_list),len(short_name_list),len(long_name_list),len(unit_list),len(temporal_res_list),len(spatial_res_list),len(Temporal_Coverage_Begin_list),len(Temporal_Coverage_End_list),len(Lat_Coverage_Begin_list),len(Lat_Coverage_End_list),len(Lon_Coverage_Begin_list),len(Lon_Coverage_End_list),len(Grid_Mapping_list),len(Make_ID_list),len(Sensor_ID_list),len(Process_ID_list),len(Study_Domain_ID_list),len(comment_list))
# print('DB: ', DB, '\n', '\n', 'Dataset_Name: ', Dataset_Name, '\n', '\n', 'Dataset_Long_Name: ',Dataset_Long_Name, '\n', '\n', 'Variables: ',Variables, '\n', '\n', 'Data_Source: ', Data_Source, '\n', '\n', 'Distributor: ', Distributor, '\n', '\n', 'Description: ', Description, '\n', '\n', 'Climatology: ',Climatology)

# print(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list,Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, keyword_list, comment_list)
# print(len(DB_list),len(Dataset_Name_list),len(short_name_list),len(long_name_list),len(unit_list),len(temporal_res_list),len(spatial_res_list),len(Temporal_Coverage_Begin_list),len(Temporal_Coverage_End_list),len(Lat_Coverage_Begin_list),len(Lat_Coverage_End_list),len(Lon_Coverage_Begin_list),len(Lon_Coverage_End_list),len(Grid_Mapping_list),len(Make_ID_list),len(Sensor_ID_list),len(Process_ID_list),len(Study_Domain_ID_list),len(comment_list))
# print(DB_list, '\n', '\n', Dataset_Name_list, '\n', '\n', short_name_list, '\n', '\n', long_name_list, '\n', '\n', unit_list, '\n', '\n',temporal_res_list, '\n', '\n', spatial_res_list, '\n', '\n', Temporal_Coverage_Begin_list, '\n', '\n', Temporal_Coverage_End_list, '\n', '\n', Lat_Coverage_Begin_list, '\n', '\n', Lat_Coverage_End_list, '\n', '\n', Lon_Coverage_Begin_list, '\n', '\n', Lon_Coverage_End_list, '\n', '\n', Grid_Mapping_list, '\n', '\n',Make_ID_list, '\n', '\n', Sensor_ID_list, '\n', '\n', Process_ID_list, '\n', '\n', Study_Domain_ID_list, '\n', '\n',  comment_list, '\n', '\n', server)

cF.tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server)
cF.tblDataset_References(Dataset_Name, reference_list,server)
cF.tblVariables(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list,Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server)
cF.tblKeywords(vars_metadata, Dataset_Name,keyword_col,tableName,server)

# """ new ssf function updates"""
# ssf.buildVarDFSmallTables(tableName,server)

cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,554)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,555)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,556)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,557)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,558)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,559)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,560)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,561)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,562)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,563)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,565)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,566)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,567)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,568)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,569)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,571)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,572)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,573)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,574)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,576)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,577)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,578)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,579)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,580)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,581)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,582)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,583)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,584)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,585)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,586)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,587)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,588)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,589)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,590)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,591)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,592)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,593)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,594)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,595)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,596)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,597)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,598)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,599)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,600)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,601)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,602)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,603)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,604)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,605)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,606)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,608)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,609)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,610)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,611)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,612)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,613)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,614)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,615)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,616)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,617)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,618)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,619)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,620)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,621)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,622)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,623)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,624)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,625)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,626)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,627)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,628)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,629)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,630)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,631)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,632)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,633)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,634)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,635)',server)
cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(136,636)',server)
## all above are seaflow and AMT cruises -- add new cruises below ##
