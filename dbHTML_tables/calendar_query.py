

import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from opedia import db

import platform
import sys

import numpy as np
import pandas as pd
import pyodbc
import pandas.io.sql as sql
#



# HTML beautifying funciton grabbed from: https://stackoverflow.com/questions/47704441/applying-styling-to-pandas-dataframe-saved-to-html-file
def write_to_html_file(df, title, filename):
    '''
    Write an entire dataframe to an HTML file with nice formatting.
    '''

    result = '''
<html>
<head>
<style>

    h2 {
        text-align: center;
        font-family: Helvetica, Arial, sans-serif;
    }
    table {
        margin-left: auto;
        margin-right: auto;
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
    }
    th, td {
        padding: 5px;
        text-align: center;
        font-family: Helvetica, Arial, sans-serif;
        font-size: 90%;
    }
    table tbody tr:hover {
        background-color: #dddddd;
    }
    .wide {
        width: 90%;
    }

</style>
</head>
<body>
    '''
    result += '<h2> %s </h2>\n' % title
    result += df.to_html(classes='wide', escape=False, index=False)
    result += '''
</body>
</html>
'''
    with open(filename, 'w') as f:
        f.write(result)



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

def findCalendar(tableName):
    cur_str = """SELECT DISTINCT [time] FROM [Opedia].[dbo].[""" + tableName + """]"""

    df = dbRead(cur_str)
    return df

tableName = 'tblPisces_NRT_Calendar'

calendar = findCalendar(tableName)
calendar.columns = ['Available_Dates']

write_to_html_file(calendar, '', '/home/nrhagen/Documents/CMAP/_static/var_calendars/' + tableName+'.html')
