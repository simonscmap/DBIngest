""" Supply parameters for every dataset """
"""func that plots with pycmap"""
""" func that exports to cmap_docs based on table name"""


import pycmap
import numpy as np
import shutil
import os
import imgkit
import geopandas
from cartopy import crs as ccrs
import cartopy.io.img_tiles as cimgt
import cartopy
import matplotlib.pyplot as plt
import cmocean
# html_outputdir = '/home/nrhagen/Documents/CMAP_docs/_static/pycmap_tutorial_viz/html/'
static_outputdir = '/home/nrhagen/Documents/CMAP_docs/_static/mission_icons/'

""" run plotting, export figs to /figure, run func that moves all into output dir + funcname"""

def pandas2geopandas(df):
    if len(df) > 2000:
        df = df.sample(2000)
    gdf = geopandas.GeoDataFrame(
    df, geometry=geopandas.points_from_xy(df.lon, df.lat))
    gdf.crs = {'init' :'epsg:4326'}
    # crs = cartopy.crs.Mollweide(central_longitude=-140)
    crs = cartopy.crs.Mollweide()

    crs_proj4 = crs.proj4_init
    gdf['geometry'] = gdf['geometry'].to_crs(crs_proj4)
    return gdf


def cartopy_sparse_map(df,table,variable,cmap,zoom_level):
    gdf = pandas2geopandas(df)
    arcgis_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    tiles = cimgt.GoogleTiles(url=arcgis_url)
    if zoom_level == 'high':
        ax = plt.axes(projection=ccrs.Mollweide())
        print('high zoom')
        # ax.set_aspect('auto')
        # bounds = gdf.geometry.total_bounds
        # print([bounds[0], bounds[2], bounds[1], bounds[3]])
        # xscale,yscale = .02,.1
        # ax.set_extent([bounds[0] - xscale*bounds[0], bounds[2]+ xscale*bounds[2], bounds[1]- yscale*bounds[1], bounds[3]+ yscale*bounds[3]],ccrs.Mollweide())
        # ax.add_image(tiles,5,interpolation='spline36')

    else:
        ax = plt.axes(projection=ccrs.Mollweide(central_longitude=-140))
        ax.set_global()
        ax.add_image(tiles,5,interpolation='spline36')


    ax.outline_patch.set_linewidth(2)
    ax.outline_patch.set_edgecolor('#424242')
    ax.set_facecolor('#424242')


    if cmap == 'solid':
        gdf.plot(ax=ax, color='#FF8C00', markersize= 3,alpha = 0.4,rasterized=True)
    else:
        gdf.plot(ax=ax, column=variable,cmap=cmap, markersize= 3,alpha = 0.6,rasterized=True)
    # plt.savefig(static_outputdir + table + '.svg',dpi=200,facecolor=ax.get_facecolor(),rasterized=True)
    plt.savefig(static_outputdir + table + '.svg',dpi=200,transparent=True,bbox_inches='tight',rasterized=True)

    plt.close()

def gridded_map(tables, variables, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2):
    from pycmap.viz import plot_map

    go = plot_map(
                 tables=tables,
                 variables=variables,
                 dt1=dt2,
                 dt2=dt2,
                 lat1=lat1,
                 lat2=lat2,
                 lon1=lon1,
                 lon2=lon2,
                 depth1=depth1,
                 depth2=depth2,
                 exportDataFlag=False,
                 show=True
                 )
    # img_name = html_2_png(variables)
    # migrate_exports(variables,img_name)



def tblGlobalDrifterProgram():
    api = pycmap.API()
    table='tblGlobalDrifterProgram'
    variable='sst'
    dt1='2000-01-01'
    dt2='2001-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=0.5
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = cmocean.cm.solar
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblAMT13_Chisholm():
    api = pycmap.API()
    table='tblAMT13_Chisholm'
    variable='pbact_Chisholm'
    dt1='2003-01-01'
    dt2='2004-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblAMT13_Chisholm():
    api = pycmap.API()
    table='tblAMT13_Chisholm'
    variable='pbact_Chisholm'
    dt1='2003-01-01'
    dt2='2004-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblCTD_Chisholm():
    api = pycmap.API()
    table='tblCTD_Chisholm'
    variable='theta_cmore'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblCruise_Temperature():
    api = pycmap.API()
    table='tblCruise_Temperature'
    variable='temperature'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = cmocean.cm.thermal
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblCruise_Salinity():
    api = pycmap.API()
    table='tblCruise_Salinity'
    variable='salinity'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = cmocean.cm.haline
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblCruise_PAR():
    api = pycmap.API()
    table='tblCruise_PAR'
    variable='PAR'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = cmocean.cm.algae
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblDeLong_HOT_metagenomics():
    api = pycmap.API()
    table='tblDeLong_HOT_metagenomics'
    variable='cruise_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblESV():
    api = pycmap.API()
    table='tblESV'
    variable='esv_chl'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblFalkor_2018():
    api = pycmap.API()
    table='tblFalkor_2018'
    variable='CTD_Chloropigment'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblGlobal_PicoPhytoPlankton():
    api = pycmap.API()
    table='tblGlobal_PicoPhytoPlankton'
    variable='prochlorococcus_abundance'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblFlombaum():
    api = pycmap.API()
    table='tblFlombaum'
    variable='prochlorococcus_abundance_flombaum'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblGLODAP():
    api = pycmap.API()
    table='tblGLODAP'
    variable='c13'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHL2A_diel_metagenomics():
    api = pycmap.API()
    table='tblHL2A_diel_metagenomics'
    variable='sra_experiment'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOE_legacy_2A():
    api = pycmap.API()
    table='tblHOE_legacy_2A'
    variable='CTD_Chloropigment'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOE_legacy_2A_Caron_Omics():
    api = pycmap.API()
    table='tblHOE_legacy_2A_Caron_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1513_HOE_legacy_2A_Dyhrman_Omics():
    api = pycmap.API()
    table='tblKM1513_HOE_legacy_2A_Dyhrman_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOE_legacy_2B():
    api = pycmap.API()
    table='tblHOE_legacy_2B'
    variable='CTD_Temperature'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOE_legacy_3():
    api = pycmap.API()
    table='tblHOE_legacy_3'
    variable='CTD_Temperature'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOE_legacy_3_Caron_Omics():
    api = pycmap.API()
    table='tblHOE_legacy_3_Caron_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOE_legacy_4():
    api = pycmap.API()
    table='tblHOE_legacy_4'
    variable='CTD_Temperature'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKOK1607_HOE_legacy_4_Dyhrman_Omics():
    api = pycmap.API()
    table='tblKOK1607_HOE_legacy_4_Dyhrman_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKOK1607_HOE_legacy_4_Dyhrman_Omics():
    api = pycmap.API()
    table='tblKOK1607_HOE_legacy_4_Dyhrman_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT273_Caron_Omics():
    api = pycmap.API()
    table='tblHOT273_Caron_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT_LAVA():
    api = pycmap.API()
    table='tblHOT_LAVA'
    variable='CTD_Temperature'
    dt1='1971-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)



def tblKOK1806_HOT_LAVA_Dyhrman_Omics():
    api = pycmap.API()
    table='tblKOK1806_HOT_LAVA_Dyhrman_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1709_mesoscope():
    api = pycmap.API()
    table='tblKM1709_mesoscope'
    variable='Station'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1709_mesoscope_CTD():
    api = pycmap.API()
    table='tblKM1709_mesoscope_CTD'
    variable='Station'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1709_mesoscope_Dyhrman_Omics():
    api = pycmap.API()
    table='tblKM1709_mesoscope_Dyhrman_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1906_Gradients3():
    api = pycmap.API()
    table='tblKM1906_Gradients3'
    variable='CTD_Temperature'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1906_Gradients3_uwayCTD():
    api = pycmap.API()
    table='tblKM1906_Gradients3_uwayCTD'
    variable='temperature'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1906_Gradients3_uw_tsg():
    api = pycmap.API()
    table='tblKM1906_Gradients3_uw_tsg'
    variable='SST'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1906_Gradients3_uway_optics():
    api = pycmap.API()
    table='tblKM1906_Gradients3_uway_optics'
    variable='SST'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKOK1606_Gradients1_uway_optics():
    api = pycmap.API()
    table='tblKOK1606_Gradients1_uway_optics'
    variable='SST'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKOK1606_Gradients1_Diazotroph():
    api = pycmap.API()
    table='tblKOK1606_Gradients1_Diazotroph'
    variable='N2_fixation_rate_mean'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblMGL1704_Gradients2_Diazotroph():
    api = pycmap.API()
    table='tblMGL1704_Gradients2_Diazotroph'
    variable='UCYN_A1_nifH_genes_mean'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblMGL1704_Gradients2_uway_optics():
    api = pycmap.API()
    table='tblMGL1704_Gradients2_uway_optics'
    variable='SST'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKM1314_Cobalmins():
    api = pycmap.API()
    table='tblKM1314_Cobalmins'
    variable='OH_PseudoCobalamin_Particulate_pM'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblKOK1606_Gradients1_Cobalamins():
    api = pycmap.API()
    table='tblKOK1606_Gradients1_Cobalamins'
    variable='Cobalamin_Particulate'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblMGL1704_Gradients2_Cobalamins():
    api = pycmap.API()
    table='tblMGL1704_Gradients2_Cobalamins'
    variable='Cobalamin_Particulate'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblSCOPE_HOT_metagenomics():
    api = pycmap.API()
    table='tblSCOPE_HOT_metagenomics'
    variable='Sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblSeaFlow():
    api = pycmap.API()
    table='tblSeaFlow'
    variable='cruise'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = cmocean.cm.thermal
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblSingleCellGenomes_Chisholm():
    api = pycmap.API()
    table='tblSingleCellGenomes_Chisholm'
    variable='cruise_name_singleCell'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'Global'
    cmap = cmocean.cm.thermal
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblAloha_Deep_Trap_Omics():
    api = pycmap.API()
    table='tblAloha_Deep_Trap_Omics'
    variable='sample_name'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)


def tblKOK1606_Gradients1_TargetedMetabolites():
    api = pycmap.API()
    table='tblKOK1606_Gradients1_TargetedMetabolites'
    variable='*'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblMGL1704_Gradients2_TargetedMetabolites():
    api = pycmap.API()
    table='tblMGL1704_Gradients2_TargetedMetabolites'
    variable='4_Hydroxyisoleucine'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT_Bottle():
    api = pycmap.API()
    table='tblHOT_Bottle'
    variable='pressure_ctd_bottle_hot'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')

def tblHOT_CTD():
    api = pycmap.API()
    table='tblHOT_CTD'
    variable='pressure_ctd_hot'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT_EpiMicroscopy():
    api = pycmap.API()
    table='tblHOT_EpiMicroscopy'
    variable='diatom_hot'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT_Macrozooplankton():
    api = pycmap.API()
    table='tblHOT_Macrozooplankton'
    variable='volume_zooplank_hot'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT_ParticleFlux():
    api = pycmap.API()
    table='tblHOT_ParticleFlux'
    variable='carbon_hot'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)

def tblHOT_PP():
    api = pycmap.API()
    table='tblHOT_PP'
    variable='chl_hot'
    dt1='1970-01-01'
    dt2='2020-01-01'
    lat1=-90
    lat2=90
    lon1=-180
    lon2=180
    depth1=0
    depth2=5000
    df = api.space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
    zoom_level = 'None'
    cmap = 'solid'
    print('Data for ' +  table + ' retrieved')
    cartopy_sparse_map(df,table,variable,cmap,zoom_level)


# tblGlobalDrifterProgram()
# tblAMT13_Chisholm()
# tblCTD_Chisholm()
# tblCruise_Temperature()
# tblCruise_Salinity()
# tblCruise_PAR()
# tblDeLong_HOT_metagenomics()
# tblESV()
# tblFalkor_2018()
# tblGlobal_PicoPhytoPlankton()
# tblFlombaum()
# tblGLODAP()
# tblHL2A_diel_metagenomics()
# tblHOE_legacy_2A()
# tblHOE_legacy_2A_Caron_Omics()
# tblKM1513_HOE_legacy_2A_Dyhrman_Omics()
# tblHOE_legacy_2B()
# tblHOE_legacy_3()
# tblHOE_legacy_3_Caron_Omics()
# tblHOE_legacy_4()
# tblKOK1607_HOE_legacy_4_Dyhrman_Omics()
# tblHOT273_Caron_Omics()
# tblHOT_LAVA()
# tblKOK1806_HOT_LAVA_Dyhrman_Omics()
# tblKM1709_mesoscope()
# tblKM1709_mesoscope_CTD()
# tblKM1709_mesoscope_Dyhrman_Omics()
# tblKM1906_Gradients3()
# tblKM1906_Gradients3_uwayCTD()
# tblKM1906_Gradients3_uw_tsg()
# tblKM1906_Gradients3_uway_optics()
# tblKOK1606_Gradients1_uway_optics()
# tblKOK1606_Gradients1_Diazotroph()
# tblMGL1704_Gradients2_Diazotroph()
# tblMGL1704_Gradients2_uway_optics()
# tblKM1314_Cobalmins()
# tblKOK1606_Gradients1_Cobalamins()
# tblMGL1704_Gradients2_Cobalamins()
# tblSCOPE_HOT_metagenomics()
# tblSeaFlow()
# tblSingleCellGenomes_Chisholm()
# tblAloha_Deep_Trap_Omics()
# tblKOK1606_Gradients1_TargetedMetabolites()
# tblMGL1704_Gradients2_TargetedMetabolites()
# tblHOT_Bottle()
# tblHOT_CTD()
# tblHOT_EpiMicroscopy()
# tblHOT_Macrozooplankton()
# tblHOT_ParticleFlux()
# tblHOT_PP()



"""altimetry reprocessed"""
# def tblAltimetry_REP():
#     tables=['tblAltimetry_REP']
#     variables=['sla']
#     dt1='1993-01-01'
#     dt2='1993-01-01'
#     lat1=-90
#     lat2=90
#     lon1=-180
#     lon2=180
#     depth1=0
#     depth2=0.5
#     gridded_map(tables, variables, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
# #
# """altimetry NRT"""
# def tblAltimetry_NRT():
#     tables=['tblAltimetry_NRT']
#     variables=['vgosa_nrt']
#     dt1='2019-01-01'
#     dt2='2019-01-01'
#     lat1=-90
#     lat2=90
#     lon1=-180
#     lon2=180
#     depth1=0
#     depth2=0.5
#     gridded_map(tables, variables, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
#
# """CHL NRT"""
# def tblCHL_REP():
#     tables=['tblCHL_REP']
#     variables=['chl']
#     dt1='1998-01-01'
#     dt2='1998-01-01'
#     lat1=-90
#     lat2=90
#     lon1=-180
#     lon2=180
#     depth1=0
#     depth2=0.5
#     gridded_map(tables, variables, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
#
# """CHL NRT"""
# def tblCHL_REP():
#     tables=['tblCHL_REP']
#     variables=['chl']
#     dt1='1998-01-01'
#     dt2='1998-01-01'
#     lat1=-90
#     lat2=90
#     lon1=-180
#     lon2=180
#     depth1=0
#     depth2=0.5
#     gridded_map(tables, variables, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
#
# """CHL NRT"""
