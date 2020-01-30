import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from opedia import db
sys.path.append('../')
import insertFunctions as iF
import plotly.graph_objects as go
import plotly


"""
generic query func, run for every dataset, then strings for names and paths for figures

Dataset name common - string
Sensor - path to img
Spatial Res - function + join
Temporal Res - function + join
Start Date - query to tblDataset_Stats
End Date - query to tblDataset_Stats

Builds pandas dataframe and apply css styling function
"""
Dataset_Name = 'SeaFlow'

# Sensor = '/home/nrhagen/Documents/CMAP_docs/_static/catalog_thumbnails/sailboat.png'
tableName = 'tblSeaFlow'




""" first test - query data, min, max dates and image into dataframe then style """


def exportData(df, path):
    if not os.path.exists(path):
        os.makedirs(path)
    iF.write_to_html_file(df, tname, path + '/' + tname + '.html')
    return

def getSensor(tableName):
    sql_query = """ SELECT [Sensor]
    FROM [Opedia].[dbo].[tblVariables] tblVar inner join tblSensors tblSn on tblVar.Sensor_ID=tblSn.ID
    WHERE tblVar.Table_Name = '""" + tableName + """'"""
    df = db.dbFetch(sql_query)
    return df

def getMake(tableName):
    sql_query = """ SELECT [Make]
    FROM [Opedia].[dbo].[tblVariables] tblVar inner join tblMakes tblMk on tblVar.Make_ID=tblMk.ID
    WHERE tblVar.Table_Name = '""" + tableName + """'"""
    df = db.dbFetch(sql_query)
    return df

def getSpatialRes(tableName):
    sql_query = """ SELECT [Spatial_Resolution]
    FROM [Opedia].[dbo].[tblVariables] tblVar inner join tblSpatial_Resolutions tblSR on tblVar.Spatial_Res_ID=tblSR.ID
    WHERE tblVar.Table_Name = '""" + tableName + """'"""
    df = db.dbFetch(sql_query)
    return df


def getTemporalRes(tableName):
    sql_query = """ SELECT [Temporal_Resolution]
    FROM [Opedia].[dbo].[tblVariables] tblVar inner join tblTemporal_Resolutions tblTR on tblVar.Temporal_Res_ID=tblTR.ID
    WHERE tblVar.Table_Name = '""" + tableName + """'"""
    df = db.dbFetch(sql_query)
    return df

def path_to_image_html(path):
    return '<img src="'+ path + '" width="60" >'

def read_JSON_Stats(query):
    df = db.dbFetch(query)
    df = pd.read_json(df['JSON_stats'][0])
    return df

sensor = getSensor(tableName)['Sensor'][0]
make = getMake(tableName)['Make'][0]
spatial_res = getSpatialRes(tableName)['Spatial_Resolution'][0]
temporal_res = getTemporalRes(tableName)['Temporal_Resolution'][0]
minDate = read_JSON_Stats(""" select * from tblDataset_Stats where Dataset_Name = '""" + tableName + "'")['time'].loc['min'].split('T',1)[0]#.replace('T', ' ')
maxDate = read_JSON_Stats(""" select * from tblDataset_Stats where Dataset_Name = '""" + tableName + "'")['time'].loc['max'].split('T',1)[0]#.replace('T', ' ')


tableDF = pd.DataFrame({'Dataset Name':Dataset_Name,'Sensor':sensor, 'Make':make, 'Spatial Resolution': spatial_res ,'Temporal Resolution': temporal_res, 'Start Date':minDate,'End Date':maxDate},index=[0])


fig = go.Figure(data=[go.Table(
    header=dict(values=list(tableDF.columns),
        line_color='#E1E4E5',
        font_size=16,
        font_family='sans-serif',
        height=35,
        fill_color='white',
        align='center'),

    cells=dict(values= [tableDF['Dataset Name'],tableDF['Sensor'],tableDF['Make'],tableDF['Spatial Resolution'],tableDF['Temporal Resolution'],tableDF['Start Date'], tableDF['End Date']],
    fill_color='#F3F6F6',
    line_color='#E1E4E5',
    font_family='sans-serif',
    font_size=16,
    height=65,
    align='center'))])
# fig.show()

plotly.offline.plot(fig,filename = '/home/nrhagen/Documents/CMAP_docs/_static/var_tables/tblSeaFlow/' + tableName + '_header_table.html', auto_open=False)
# tableDF.to_html('/home/nrhagen/Desktop/' + tableName + '_header_table.html', escape=False,formatters=dict(Sensor=path_to_image_html))
# iF.write_to_html_file(tableDF, tableName, '/home/nrhagen/Desktop/' + tableName + '_header_table.html',header=True, title_opt=False)
""" ex: df['lat'].loc['min']"""











# tablename_list = catalog(dirPath, table_name_query)['Dataset_Name'].tolist()
# tablename_ID = catalog(dirPath, table_name_query)['Dataset_ID'].tolist()
#
# for tname,id in zip(tablename_list, tablename_ID):
#     var_df = catalog(dirPath, query + """ WHERE [Dataset_ID] = """ + str(id) )
#     var_df = var_df[['Table_Name', 'Short_Name', 'Long_Name', 'Unit']]
#     exportData(var_df, OUTPUTDIR + tname)
