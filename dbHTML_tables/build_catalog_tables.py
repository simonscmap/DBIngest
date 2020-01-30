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
sys.path.append('../')
import insertFunctions as iF

dirPath = 'data/'
OUTPUTDIR = '/home/nrhagen/Documents/CMAP_docs/_static/var_tables/'

table_name_query = """ SELECT [ID] as Dataset_ID, [Dataset_Name] FROM [Opedia].[dbo].[tblDatasets] """



query = """ Select
   [DB]
  ,[Table_Name]
  ,[Short_Name]
  ,[Long_Name]
  ,[Unit]
  ,[Dataset_ID]
FROM [Opedia].[dbo].[tblVariables]"""


def exportData(df, path):
    if not os.path.exists(path):
        os.makedirs(path)
    iF.write_to_html_file(df, tname, path + '/' + tname + '.html')
    return

def catalog(dirPath, query):
    df = db.dbFetch(query)
    return df


tablename_list = catalog(dirPath, table_name_query)['Dataset_Name'].tolist()
tablename_ID = catalog(dirPath, table_name_query)['Dataset_ID'].tolist()

for tname,id in zip(tablename_list, tablename_ID):
    var_df = catalog(dirPath, query + """ WHERE [Dataset_ID] = """ + str(id) )
    var_df = var_df[['Table_Name', 'Short_Name', 'Long_Name', 'Unit']]
    exportData(var_df, OUTPUTDIR + tname)
