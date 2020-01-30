""" script that creates variable table for each database table in catalog
create list from selection of [Opedia].[dbo].[tblDatasets]
loop through dataset list
select * from [Opedia].[dbo].[tblVariables] where DB name is [dataset]
export html into catalog/

 """

import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from opedia import db
import imgkit

import platform
import sys

import numpy as np
import pandas as pd
import pyodbc
import pandas.io.sql as sql
#

dirPath = 'data/'
OUTPUTDIR = '/home/nrhagen/Documents/CMAP/_static/var_tables/'

table_name_query = """ SELECT [ID] as Dataset_ID, [Dataset_Name] FROM [Opedia].[dbo].[tblDatasets] """
options = {'format': 'png','crop-h': '3','crop-w': '3','crop-x': '3','crop-y': '3','encoding': "UTF-8",'no-outline': None}

def write_to_html_file(df, title, filename):
    result = df.to_html(index=False).replace('border="1"','border="0"')

    with open(filename, 'w') as f:
        f.write(result)
    import imgkit
def html_to_img(filename):
    imgkit.from_file(filename + '.html', filename + '.png')

query = """ Select
   [DB]
  ,[Table_Name]
  ,[Short_Name]
  ,[Long_Name]
  ,[Unit]
  ,[Dataset_ID]
FROM [Opedia].[dbo].[tblVariables]"""


def dbConnect(usr='ArmLab', psw='ArmLab2018', ip='128.208.239.15', port='1433', db='Opedia', TDS_Version='7.3'):
        try:
            server = ip + ',' + port
            if platform.system().lower().find('windows') != -1:
                conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Uid=' + usr + ';Pwd='+ psw )
            elif platform.system().lower().find('darwin') != -1:
                conn = pyodbc.connect('DRIVER=/usr/local/lib/libtdsodbc.so;SERVER=' + server + ';DATABASE=' + db + ';Uid=' + usr + ';Pwd='+ psw )
            elif platform.system().lower().find('linux') != -1:
                conn = pyodbc.connect(DRIVER='/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so', TDS_Version =  TDS_Version , server =  ip , port =  port, uid = usr, pwd = psw)
            #print('Successful database connection')
        except Exception as e:
            print('Database connection error. Error message: '+str(e))
        return conn


def dbRead(query):
    conn = dbConnect()
    dframe = sql.read_sql(query, conn)
    conn.close()
    return dframe

def findMinDate(tableName):
    cur_str = """select min(time) FROM [Opedia].[dbo].[""" + tableName + """]"""
    df = dbRead(cur_str)
    return df
    # dates = df.iloc[0].values
    # minDate = pd.to_datetime(str(dates[0])).strftime('%Y-%m-%d')
    # maxDate = pd.to_datetime(str(dates[1])).strftime('%Y-%m-%d')
    # return {'minDate':minDate,'maxDate':maxDate}


def exportData(df, path):
    if not os.path.exists(path):
        os.makedirs(path)
    write_to_html_file(df, tname, path + '/' + tname + '.html')

    return

Dataset_Name = 'tblModis_AOD_REP'

minDate = findMinDate(Dataset_Name)
# minDate.to_html('test.html')

# minDate.to_html(classes='wide', escape=False).replace('border="1"','border="0"')

write_to_html_file(minDate,'', Dataset_Name + '.html')
html_to_img(Dataset_Name, options=options)

# def catalog(dirPath, query):
#     df = db.dbFetch(query)
#     return df

#
# tablename_list = catalog(dirPath, table_name_query)['Dataset_Name'].tolist()
# tablename_ID = catalog(dirPath, table_name_query)['Dataset_ID'].tolist()
#
# for tname,id in zip(tablename_list, tablename_ID):
#     var_df = catalog(dirPath, query + """ WHERE [Dataset_ID] = """ + str(id) )
#     exportData(var_df, OUTPUTDIR + tname)
