import sys
sys.path.append('../../config')
import config_vault as cfgv
sys.path.append('../../lib')
import dateLib as dl
sys.path.append('../')
import dbCore as dc
import os
import netcdf as nc
import numpy as np
import pandas as pd
import pyodbc
import pandas.io.sql as sql
from datetime import datetime
import time
import math
import insertPrep as ip



def formatDate_Columns(df, dateLabels, fmt='%m%d%y'):
    date_len = 6    
    for dateLabel in dateLabels:
        date_ind = df.columns.get_loc(dateLabel)
        df[dateLabel] =df[dateLabel].astype(str)
        df[dateLabel] = df[dateLabel].str.strip()
        for i in range(len(df)):        
            df.iloc[i, date_ind] = ('0'*(date_len-len(df.iloc[i, date_ind]))) + df.iloc[i, date_ind]
        df[dateLabel] =  pd.to_datetime(df[dateLabel], format=fmt)
    return df

def formatTime_Columns(df, timeLabels, fmt='%H%M'):
    time_len = 4
    for timeLabel in timeLabels:
        time_ind = df.columns.get_loc(timeLabel)
        for i in range(len(df)):  
            if len(df.iloc[i, time_ind]) > 1 : 
                df.iloc[i, time_ind] = ('0'*(time_len-len(df.iloc[i, time_ind]))) + df.iloc[i, time_ind]
        #df[timeLabel] =  pd.to_datetime(df[timeLabel], format=fmt, errors='ignore')
    return df


def makeBulkMacrozooplankton_HOT():
    path = cfgv.rep_hot_raw + 'macrozooplankton.csv'    
    prefix = 'mz_hot'
    missingValue = -9 
    df = pd.read_csv(path)   
    if ' ' in df.columns:
        df.drop(' ', axis=1, inplace=True)
    df = df.drop(df.index[[0]])   ## remove the units row    
    df.columns = df.columns.str.replace(' ','')   
    df = df.apply(pd.to_numeric)
    df = df.replace(missingValue, '')

    formatDate_Columns(df, ['date'])
    #formatTime_Columns(df, ['stime', 'etime'])

    ## Are the below lat/lon values correct?
    df['lat'] = 22.75
    df['lon'] = -158
    df['ID'] = None
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s.csv' % (exportBase, prefix)
    df.to_csv(export_path, index=False)
    return export_path



def bulkInsertMacrozooplankton_HOT(tableName):
    dataTitle = 'HOT_Macrozooplankton'
    print('%s  Inserting Bulk %s into %s.' % (datetime.today(), dataTitle, tableName))
    try:
        bulkPath = ''
        bulkPath = makeBulkMacrozooplankton_HOT()
        dc.bulkInsert(bulkPath, tableName)
    finally:
        if bulkPath != '':
            os.remove(bulkPath)    
    print('%s  Done' % datetime.today())
    return



tableName = 'tblHOT_Macrozooplankton'
bulkInsertMacrozooplankton_HOT(tableName)

