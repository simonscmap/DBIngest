

import sys
sys.path.append('../')
import insertFunctions as iF
import insertPrep as ip
import config_vault as cfgv
import pandas as pd
import glob
import xarray as xr
import os.path
import shutil
import numpy as np
import matplotlib.pyplot as plt
import rioxarray
sys.path.append('../summary_stats/')

import summary_stats_func as ssf

############################
########### OPTS ###########
tableName = 'tblNOAA_NSIDC_CDR_Sea_Ice'
rawFilePath = cfgv.rep_NOAA_NSIDC_CDR_SeaIce_raw
filelist = glob.glob(rawFilePath + '*.nc')
server = 'Rainier'

# finished_file_list = glob.glob(cfgv.opedia_proj + 'db/dbInsert/export/' + '*tblNOAA*.nc*')
# csv_removed_file_list = [x[:-4] for x in finished_file_list]
#
# base_filelist  = [os.path.basename(x) for x in filelist]
# base_csv_removed_file_list = [os.path.basename(x) for x in csv_removed_file_list]
# base_csv_removed_file_list = [x[26:] for x in base_csv_removed_file_list]
#
# new_list = list(np.sort(list(set(base_filelist).difference(base_csv_removed_file_list))))

def clean_nsidc_dir():
    os.chdir(rawFilePath)
    ns_dirs = os.listdir()
    for directory in ns_dirs:
        os.chdir(rawFilePath +directory)
        yearfilelist = os.listdir()
        for yearfile in yearfilelist:
            print(yearfile)
            os.chdir(rawFilePath +directory + '/' + yearfile)
            files = os.listdir(rawFilePath +directory + '/' + yearfile)
            # print(files)
            for f in files:
                shutil.move(rawFilePath +directory + '/' + yearfile +'/'+ f, rawFilePath)



# clean_nsidc_dir()
exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
prefix = tableName
export_path = '%s%s.csv' % (exportBase, prefix)

# ############################
# ############################




def makeNOAA_NSIDC_SeaIce(rawFilePath, rawFileName, tableName):
    usecols = ['seaice_conc_cdr','goddard_merged_seaice_conc','goddard_nt_seaice_conc','goddard_bt_seaice_conc','stdev_of_seaice_conc_cdr','qa_of_seaice_conc_cdr']
    path = rawFilePath + rawFileName
    # print(path)

    prefix = tableName
    exportBase = cfgv.opedia_proj + 'db/dbInsert/export/'
    export_path = '%s%s_%s.csv' % (exportBase, prefix, rawFileName)
    # print(export_path)
    xds = rioxarray.open_rasterio(path)
    xds_4326 = xds.rio.reproject("epsg:4326'")
    df = xds_4326.to_dataframe()
    df.reset_index(inplace=True)

    # df, xdf = ip.netcdf_2_dataframe(path, usecols = usecols)
    # return df,xdf
    # return df,xdf

    df['time'] = pd.to_datetime(df['time'].astype(str)).dt.strftime('%Y-%m-%d %H:%M:%S')

    df = ip.renameCol(df,'latitude', 'lat')
    df = ip.renameCol(df,'longitude', 'lon')
    df = df[['time','lat','lon'] + usecols]

    df = ip.colDatatypes(df)
    df = df.replace(255, '')
    df = df.replace(254, '')
    df = df.replace(253, '')
    df = df.replace(-999.000000, '')
    df = ip.removeMissings(['time','lat', 'lon'], df)
    df = ip.NaNtoNone(df)
    df = ip.removeDuplicates(df)

    # ssf.dataframe_describe_write(df, tableName)
    df.to_csv(export_path, index=False)
    ip.sortByTimeLatLon(df, export_path, 'time', 'lat', 'lon')
    print('export path: ' ,export_path)
    return export_path


# rawFileName = '/mnt/vault/observation/remote/satellite/NOAA_NSIDC_CDR_SeaIce/rep/seaice_conc_daily_sh_f17_20110111_v03r01.nc'#filelist[0]
# df,xdf = makeNOAA_NSIDC_SeaIce(rawFileName, os.path.basename(rawFileName), tableName)
# df, xdf = makeNOAA_NSIDC_SeaIce(rawFileName, os.path.basename(rawFileName), tableName)
# df = makeNOAA_NSIDC_SeaIce(rawFileName, os.path.basename(rawFileName), tableName)
# export_path = makeNOAA_NSIDC_SeaIce(rawFilePath, os.path.basename(rawFileName), tableName)
# iF.toSQLbcp(export_path, tableName,server)

rawFileName = '/mnt/vault/observation/remote/satellite/NOAA_NSIDC_CDR_SeaIce/rep/seaice_conc_daily_nh_n07_20120101_v03r01.nc'
rawFileName = '/mnt/vault/observation/remote/satellite/NOAA_NSIDC_CDR_SeaIce/rep/seaice_conc_daily_nh_f13_20020122_v03r01.nc'
# df = makeNOAA_NSIDC_SeaIce(rawFilePath, os.path.basename(rawFileName), tableName)

export_path = makeNOAA_NSIDC_SeaIce(rawFilePath, os.path.basename(rawFileName), tableName)
iF.toSQLbcp(export_path, tableName,server)

# for rawFileName in filelist:
#     print(rawFileName)
#     break
    # print(rawFileName)
    # # df,xdf = makeNOAA_NSIDC_SeaIce(rawFilePath, os.path.basename(rawFileName), tableName)
    # export_path = makeNOAA_NSIDC_SeaIce(rawFilePath, os.path.basename(rawFileName), tableName)
    # print(export_path)
    # iF.toSQLbcp(export_path, tableName,server)

# df['seaice_conc_cdr'].plot()
# df['goddard_merged_seaice_conc'].plot()
# df['goddard_nt_seaice_conc'].plot()
# df['goddard_bt_seaice_conc'].plot()
# plt.show()
