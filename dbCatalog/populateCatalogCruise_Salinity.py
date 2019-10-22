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

"""Cruise_Salinity data Catalog Table"""
server = 'Rainier'
tableName = 'tblCruise_Salinity'
rawFilePath = cfgv.rep_Cruise_Salinity_raw
rawFileName = 'cruise_salinity_metadata.xlsx'
keyword_col = 'var_keywords'

############################



dataset_metadata = pd.read_excel(rawFilePath + rawFileName, sheet_name = 0)
dataset_metadata = ip.removeLeadingWhiteSpace(dataset_metadata)
vars_metadata = pd.read_excel(rawFilePath + rawFileName,sheet_name = 1)


""" Strings """
DB='Opedia'
Dataset_Name = 'tblCruise_Salinity'
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
Dataset_Name_list = [Dataset_Name] * len(vars_metadata)
short_name_list = list(vars_metadata['var_short_name'])
long_name_list = list(vars_metadata['var_long_name'])
unit_list = list(ip.NaNtoNone(vars_metadata['var_unit']))

spatial_res_list = list('1') * len(vars_metadata)# Irregular
temporal_res_list = list('7') * len(vars_metadata) # Irregular

comment_list = list(ip.NaNtoNone(vars_metadata['var_comment']))
Temporal_Coverage_Begin_list = [cF.findMinMaxDate(Dataset_Name,server)['minDate']]  * len(vars_metadata)
Temporal_Coverage_End_list = [cF.findMinMaxDate(Dataset_Name,server)['maxDate']] * len(vars_metadata)
Lat_Coverage_Begin_list = [cF.findSpatialBounds(Dataset_Name,server)['minLat']] * len(vars_metadata)
Lat_Coverage_End_list = [cF.findSpatialBounds(Dataset_Name,server)['maxLat']] * len(vars_metadata)
Lon_Coverage_Begin_list = [cF.findSpatialBounds(Dataset_Name,server)['minLon']] * len(vars_metadata)
Lon_Coverage_End_list = [cF.findSpatialBounds(Dataset_Name,server)['maxLon']] * len(vars_metadata)
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

# cF.tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server)
# cF.tblDataset_References(Dataset_Name, reference_list,server)
# cF.tblVariables(DB_list, Dataset_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list,Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server)
# cF.tblKeywords(vars_metadata, Dataset_Name,keyword_col,tableName,server)
# #
# """ new ssf function updates"""
# ssf.buildVarDFSmallTables(tableName,server)


# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,554)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,555)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,556)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,557)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,558)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,559)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,560)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,561)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,562)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,563)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,565)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,566)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,567)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,568)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,569)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,571)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,572)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,573)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,574)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,576)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,577)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,578)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,579)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,580)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,581)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,582)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,583)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,584)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,585)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,586)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,587)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,588)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,589)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,590)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,591)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,592)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,593)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,594)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,595)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,596)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,597)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,598)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,599)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,600)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,601)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,602)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,603)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,604)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,605)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,606)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,608)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,609)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,610)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,611)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,612)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,613)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,614)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,615)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,616)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,617)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,618)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,619)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,620)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,621)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,622)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,623)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,624)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,625)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,626)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,627)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,628)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,629)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,630)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,631)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,632)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,633)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,634)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,635)',server)
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(131,636)',server)
### all above are seaflow and AMT cruises -- add new cruises below ##
