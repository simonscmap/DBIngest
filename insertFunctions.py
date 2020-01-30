import os, os.path
import sys
import xarray as xr
sys.path.append('../login')
sys.path.append('../../config')
import pandas as pd
# import config_vault as cfgv
import credentials as cr
sys.path.append('../')
import dbCore as dc

def toSQLbcp(export_path, tableName,  server):
    if server == 'Rainier':
        usr=cr.usr_rainier
        psw=cr.psw_rainier
        ip=cr.ip_rainier
        port = cr.port_rainier
    else:
        usr=cr.usr_beast
        psw=cr.psw_beast
        ip=cr.ip_beast
        port = cr.port_beast

    print('Inserting Bulk %s into %s.' % (tableName[3:], tableName))
    str = """bcp Opedia.dbo.""" + tableName + """ in """ + export_path + """ -e error -c -t, -U  """ + usr + """ -P """ + psw + """ -S """ + ip + """,""" + port
    os.system(str)
    print('BCP insert finished')

def lineInsert(server, tableName, columnList ,query):
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    insertQuery = """INSERT INTO %s %s VALUES %s """ % (tableName, columnList, query)
    print(insertQuery)
    cursor.execute(insertQuery)
    conn.commit()


def netcdf_2_dataframe(netcdf_file,usecols = None):
    xdf = xr.open_dataset(netcdf_file)
    if usecols != None:
        xdf = xdf[usecols]
    df = xdf.to_dataframe()
    df.reset_index(inplace=True)
    return df, xdf

def findID_CRUISE(cruiseName):
    """ this function pulls the ID value from the [tblCruises]"""
    server = 'Rainier'
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblCruise] WHERE [Name] like '%""" + cruiseName + """%'"""
    cursor.execute(cur_str)
    IDvar = (cursor.fetchone()[0])
    return IDvar

def cruise_ID_list():
    server = 'Rainier'
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str = """select [ID], [Nickname] FROM [Opedia].[dbo].[tblCruise]"""
    cursor.execute(cur_str)
    IDlist = (cursor.fetchall())
    newID_list = [x[0] for x in IDlist]
    newNickname_list = [x[1] for x in IDlist]

    return {'ID_list':newID_list,'Nickname_list':newNickname_list}


def findMinMaxDate_cruiseID(ID):
    server = 'Rainier'
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str_min = """select min(time) FROM [Opedia].[dbo].[tblCruise_Trajectory] where Cruise_ID = '""" + str(ID) + """'"""
    cur_str_max = """select max(time) FROM [Opedia].[dbo].[tblCruise_Trajectory] where Cruise_ID = '""" + str(ID) + """'"""
    cursor.execute(cur_str_min)
    minDate = (cursor.fetchone()[0])
    cursor.execute(cur_str_max)
    maxDate = (cursor.fetchone()[0])
    return {'minDate':minDate,'maxDate':maxDate}

def findMinMaxSpatial_cruiseID(ID):
    server = 'Rainier'
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str_minlat = """select min(lat) FROM [Opedia].[dbo].[tblCruise_Trajectory] where Cruise_ID = '""" + str(ID) + """'"""
    cur_str_maxlat = """select max(lat) FROM [Opedia].[dbo].[tblCruise_Trajectory] where Cruise_ID = '""" + str(ID) + """'"""
    cur_str_minlon = """select min(lon) FROM [Opedia].[dbo].[tblCruise_Trajectory] where Cruise_ID = '""" + str(ID) + """'"""
    cur_str_maxlon = """select max(lon) FROM [Opedia].[dbo].[tblCruise_Trajectory] where Cruise_ID = '""" + str(ID) + """'"""
    cursor.execute(cur_str_minlat)
    minlat = (cursor.fetchone()[0])
    cursor.execute(cur_str_maxlat)
    maxlat = (cursor.fetchone()[0])
    cursor.execute(cur_str_minlon)
    minlon = (cursor.fetchone()[0])
    cursor.execute(cur_str_maxlon)
    maxlon = (cursor.fetchone()[0])
    return {'minlat':minlat,'maxlat':maxlat,'minlon':minlon,'maxlon':maxlon}

def findMinMaxDate(tableName):
    cur_str = 'select min(time), max(time) FROM [Opedia].[dbo].[' + tableName + ']'
    df = dc.dbRead(cur_str)
    dates = df.iloc[0].values
    minDate = pd.to_datetime(str(dates[0])).strftime('%Y-%m-%d')
    maxDate = pd.to_datetime(str(dates[1])).strftime('%Y-%m-%d')
    return {'minDate':minDate,
     'maxDate':maxDate}

def findSpatialBounds(tableName):
    cur_str = 'select min(lat), max(lat), min(lon), max(lon) FROM [Opedia].[dbo].[' + tableName + ']'
    df = dc.dbRead(cur_str)
    dates = df.iloc[0].values
    return {'minLat':dates[0],
     'maxLat':dates[1],  'minLon':dates[2],  'maxLon':dates[3]}

# HTML beautifying funciton grabbed from: https://stackoverflow.com/questions/47704441/applying-styling-to-pandas-dataframe-saved-to-html-file
def write_to_html_file(df, title, filename,header=True, title_opt=False):
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
    if title_opt == True:
        result += '<h2> %s </h2>\n' % title
    else:
        result += df.to_html(classes='wide', escape=False, index=False,header=header)
        result += '''
</body>
</html>
'''
    with open(filename, 'w') as f:
        f.write(result)
