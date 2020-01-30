
import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from opedia import db
sys.path.append('../')
import insertFunctions as iF

OUTPUTDIR = '/home/nrhagen/Documents/CMAP_docs/_static/var_depths/'


darwin_query = """ Select
   [DB]
  ,[Table_Name]
  ,[Short_Name]
  ,[Long_Name]
  ,[Unit]
  ,[Dataset_ID]
FROM [Opedia].[dbo].[tblVariables]"""

pisces_query = """ SELECT [depth_level] FROM [Opedia].[dbo].[tblPisces_Depth]"""
darwin_query = """ SELECT [depth_level] FROM [Opedia].[dbo].[tblDarwin_Depth]"""

def exportData(df, path,cal_name):
    if not os.path.exists(path):
        os.makedirs(path)
    iF.write_to_html_file(df, 'Available Depths', path + '/' + cal_name + '_depth.html')
    return

def retrieveCalendar(cal_name, cal_query, OUTPUTDIR):
    df = db.dbFetch(cal_query)
    exportData(df,OUTPUTDIR,cal_name)

retrieveCalendar('Pisces', pisces_query, OUTPUTDIR)
retrieveCalendar('Darwin', darwin_query, OUTPUTDIR)
