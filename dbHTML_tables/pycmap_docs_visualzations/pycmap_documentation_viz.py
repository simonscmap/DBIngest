import pycmap
import shutil
import os



html_outputdir = '/home/nrhagen/Documents/CMAP_docs/_static/pycmap_tutorial_viz/html/'
static_outputdir = '/home/nrhagen/Documents/CMAP_docs/_static/pycmap_tutorial_viz/static/'

""" run plotting, export figs to /figure, run func that moves all into output dir + funcname"""

def migrate_exports(funcname):
    files = os.listdir('figure/')
    print(files)
    for file in files:
        shutil.move('figure/'+ file,html_outputdir  + funcname + '_' + file)


def histogram():
  from pycmap.viz import plot_hist
  go = plot_hist(
                tables=['tblFalkor_2018', 'tblDarwin_Nutrient_Climatology', 'tblWOA_Climatology'],
                variables=['CTD_Oxygen', 'O2_darwin_clim', 'oxygen_WOA_clim'],
                dt1='2018-03-01',
                dt2='2018-04-30',
                lat1=21,
                lat2=25,
                lon1=-161,
                lon2=155,
                depth1=0,
                depth2=100,
                exportDataFlag=False,
                show=True
                )
  migrate_exports('histogram')


def timeseries():

    from pycmap.viz import plot_timeseries

    go = plot_timeseries(
                        tables=['tblAltimetry_REP', 'tblSSS_NRT'],
                        variables=['sla', 'sss'],
                        dt1='2016-04-30',
                        dt2='2017-04-30',
                        lat1=30,
                        lat2=32,
                        lon1=-160,
                        lon2=-158,
                        depth1=0,
                        depth2=0,
                        exportDataFlag=False,
                        show=True,
                        interval='w'
                        )
    migrate_exports('time_series')
    # here is how to modify a graph:

    go[0].line = False
    go[0].msize = 10
    go[0].color='#FF0023'
    go[0].title= "graph's title"
    go[0].width = 600
    go[0].height = 600
    go[0].render()

    # the graph data is held in the 'data' property
    # go[0].data
    migrate_exports('time_series_modified')

def gridded_map():
    from pycmap.viz import plot_map

    go = plot_map(
                 tables=['tblWOA_Climatology', 'tblPisces_NRT'],
                 variables=['phosphate_WOA_clim', 'Fe'],
                 dt1='2016-04-30',
                 dt2='2016-04-30',
                 lat1=10,
                 lat2=70,
                 lon1=-180,
                 lon2=-80,
                 depth1=0,
                 depth2=0.5,
                 exportDataFlag=False,
                 show=True
                 )
    migrate_exports('gridded_map')
    # here is how to modify a graph:

    go[1].cmap = 'PRGn'
    go[1].vmin = 0
    go[1].vmax = 5e-5
    go[1].width = 900
    go[1].height = 700
    go[1].render()
    migrate_exports('gridded_map_modified')

def sparse_map():
    from pycmap.viz import plot_map

    plot_map(
            tables=['tblGlobal_PicoPhytoPlankton'],
            variables=['synechococcus_abundance'],
            dt1='1990-01-30',
            dt2='1995-12-30',
            lat1=10,
            lat2=70,
            lon1=-180,
            lon2=80,
            depth1=0,
            depth2=100,
            exportDataFlag=False,
            show=True
            )
    migrate_exports('sparse_map')

def contour_map():
    #!pip install pycmap -q     #uncomment to install pycmap, if necessary
    # uncomment the lines below if the API key has not been registered on your machine, previously.
    # import pycmap
    # pycmap.API(token='YOUR_API_KEY>')

    from pycmap.viz import plot_map

    go = plot_map(
                 tables=['tblsst_AVHRR_OI_NRT'],
                 variables=['sst'],
                 dt1='2016-04-30',
                 dt2='2016-04-30',
                 lat1=10,
                 lat2=70,
                 lon1=-180,
                 lon2=-80,
                 depth1=0,
                 depth2=0,
                 exportDataFlag=False,
                 show=True,
                 levels=10
                 )
    migrate_exports('contour_map')

def threeD():
    from pycmap.viz import plot_map

    go = plot_map(
                 tables=['tblPisces_NRT'],
                 variables=['NO3'],
                 dt1='2016-04-30',
                 dt2='2016-04-30',
                 lat1=-90,
                 lat2=90,
                 lon1=-180,
                 lon2=180,
                 depth1=0,
                 depth2=0.5,
                 exportDataFlag=False,
                 show=True,
                 surface3D=True
                 )
    migrate_exports('3D_surface')


def section_map():

    from pycmap.viz import plot_section

    go = plot_section(
                     tables=['tblPisces_NRT'],
                     variables=['NO3'],
                     dt1='2016-04-30',
                     dt2='2016-04-30',
                     lat1=10,
                     lat2=60,
                     lon1=-160,
                     lon2=-158,
                     depth1=0,
                     depth2=5000,
                     exportDataFlag=False,
                     show=True
                     )
    migrate_exports('section_map')
    # here is how to modify a graph:

    import cmocean

    go[0].cmap = cmocean.cm.balance
    go[0].vmin = 0
    go[0].vmax = 60
    go[0].width = 700
    go[0].height = 800
    go[0].render()
    migrate_exports('section_map_modified')

def section_contour():
    from pycmap.viz import plot_section

    plot_section(
                tables=['tblDarwin_Nutrient'],
                variables=['SIO2'],
                dt1='2008-01-05',
                dt2='2008-01-05',
                lat1=-50,
                lat2=-46,
                lon1=-180,
                lon2=180,
                depth1=0,
                depth2=2000,
                exportDataFlag=False,
                show=True,
                levels=10
                )
    migrate_exports('section_contour')
    
def depth_profile():
    from pycmap.viz import plot_depth_profile

    go = plot_depth_profile(
                           tables=['tblArgoMerge_REP',  'tblDarwin_Ecosystem'],
                           variables=['argo_merge_chl_adj', 'CHL'],
                           dt1='2014-04-25',
                           dt2='2014-04-30',
                           lat1=20,
                           lat2=24,
                           lon1=-170,
                           lon2=-150,
                           depth1=0,
                           depth2=1500,
                           exportDataFlag=False,
                           show=True
                           )
    migrate_exports('depth_profile')
    # here is how to modify a graph:

    go[0].msize = 10
    go[0].color='#FF0023'
    go[0].title= "graph's title"
    go[0].width = 600
    go[0].height = 600
    go[0].fillAlpha=0.8
    go[0].xlabel = 'change xlabel'
    go[0].ylabel = 'change ylabel'
    go[0].render()

    # the graph data is held in the 'data' property
    # go[0].data
    migrate_exports('depth_profile_modified')

def cruise_track():

    from pycmap.viz import plot_cruise_track
    plot_cruise_track(['KM1712', 'gradients_1'])
    migrate_exports('cruise_track')

def correlation_matrix():

    from collections import namedtuple
    from pycmap.viz import plot_corr_map


    def match_params():
        Param = namedtuple('Param', ['table', 'variable', 'temporalTolerance', 'latTolerance', 'lonTolerance', 'depthTolerance'])
        params = []
        ######## self-matching: colocalizing with some other variables in the tblAMT13_Chisholm dataset
        params.append(Param('tblAMT13_Chisholm', 'MIT9312PCR_Chisholm', 0, 0, 0, 0))
        params.append(Param('tblAMT13_Chisholm', 'MED4PCR_Chisholm', 0, 0, 0, 0))
        params.append(Param('tblAMT13_Chisholm', 'sbact_Chisholm', 0, 0, 0, 0))
        ####### WOA: World Ocean Atlas Monthly Climatology
        params.append(Param('tblWOA_Climatology', 'nitrate_WOA_clim', 0, .5, .5, 5))
        params.append(Param('tblWOA_Climatology', 'phosphate_WOA_clim', 0, 0.5, 0.5, 5))
        ####### Satellite
        params.append(Param('tblCHL_REP', 'chl', 4, 0.25, 0.25, 0))
        ####### Darwin Model
        params.append(Param('tblDarwin_Phytoplankton', 'picoprokaryote', 2, 0.25, 0.25, 5))


        tables, variables, temporalTolerance, latTolerance, lonTolerance, depthTolerance = [], [], [], [], [], []
        for i in range(len(params)):
            tables.append(params[i].table)
            variables.append(params[i].variable)
            temporalTolerance.append(params[i].temporalTolerance)
            latTolerance.append(params[i].latTolerance)
            lonTolerance.append(params[i].lonTolerance)
            depthTolerance.append(params[i].depthTolerance)
        return tables, variables, temporalTolerance, latTolerance, lonTolerance, depthTolerance



    targetTables, targetVars, temporalTolerance, latTolerance, lonTolerance, depthTolerance = match_params()
    go = plot_corr_map(
                      sourceTable='tblAMT13_Chisholm',
                      sourceVar='MIT9313PCR_Chisholm',
                      targetTables=targetTables,
                      targetVars=targetVars,
                      dt1='2003-09-14',
                      dt2='2003-10-13',
                      lat1=-48,
                      lat2=48,
                      lon1=-52,
                      lon2=-11,
                      depth1=0,
                      depth2=240,
                      temporalTolerance=temporalTolerance,
                      latTolerance=latTolerance,
                      lonTolerance=lonTolerance,
                      depthTolerance=depthTolerance
                      )
    migrate_exports('correlation_matrix')
    import numpy as np

    go.z = np.abs(go.z)
    go.cmap = 'Greys'
    go.width = 1000
    go.height = 1000
    go.render()
    migrate_exports('correlation_matrix_modified')
def correlation_matrix_cruise_track():

    from collections import namedtuple
    from pycmap.viz import plot_cruise_corr_map


    def match_params():
        Param = namedtuple('Param', ['table', 'variable', 'temporalTolerance', 'latTolerance', 'lonTolerance', 'depthTolerance'])
        params = []
        ######## seaflow
        params.append(Param('tblSeaFlow', 'prochloro_abundance', 0, 0.1, 0.1, 5))
        params.append(Param('tblSeaFlow', 'synecho_abundance', 0, 0.1, 0.1, 5))
        params.append(Param('tblSeaFlow', 'picoeuk_abundance', 0, 0.1, 0.1, 5))
        ####### WOA: World Ocean Atlas Monthly Climatology
        params.append(Param('tblWOA_Climatology', 'silicate_WOA_clim', 0, .5, .5, 5))
        params.append(Param('tblWOA_Climatology', 'oxygen_WOA_clim', 0, 0.5, 0.5, 5))
        ####### Satellite
        params.append(Param('tblSST_AVHRR_OI_NRT', 'sst', 1, 0.25, 0.25, 5))
        params.append(Param('tblSSS_NRT', 'sss', 1, 0.25, 0.25, 5))
        params.append(Param('tblAltimetry_REP', 'adt', 1, 0.25, 0.25, 5))
        params.append(Param('tblCHL_REP', 'chl', 4, 0.25, 0.25, 0))
        ####### Models
        params.append(Param('tblPisces_NRT', 'NO3', 4, 0.5, 0.5, 5))
        params.append(Param('tblDarwin_Plankton_Climatology', 'prokaryote_c01_darwin_clim', 0, 0.5, 0.5, 5))
        params.append(Param('tblDarwin_Plankton_Climatology', 'prokaryote_c02_darwin_clim', 0, 0.5, 0.5, 5))

        tables, variables, temporalTolerance, latTolerance, lonTolerance, depthTolerance = [], [], [], [], [], []
        for i in range(len(params)):
            tables.append(params[i].table)
            variables.append(params[i].variable)
            temporalTolerance.append(params[i].temporalTolerance)
            latTolerance.append(params[i].latTolerance)
            lonTolerance.append(params[i].lonTolerance)
            depthTolerance.append(params[i].depthTolerance)
        return tables, variables, temporalTolerance, latTolerance, lonTolerance, depthTolerance



    targetTables, targetVars, temporalTolerance, latTolerance, lonTolerance, depthTolerance = match_params()
    go = plot_cruise_corr_map(
                             cruise='MGL1704', # Gradients_2
                             targetTables=targetTables,
                             targetVars=targetVars,
                             depth1=0,
                             depth2=5,
                             temporalTolerance=temporalTolerance,
                             latTolerance=latTolerance,
                             lonTolerance=lonTolerance,
                             depthTolerance=depthTolerance
                             )
    migrate_exports('correlation_matrix_cruise_track')
    import numpy as np

    # print correlation values
    # print(go.z)
    # print(go.x)
    # print(go.y)
    go.z = np.abs(go.z)
    go.cmap = 'Greys'
    go.width = 1000
    go.height = 1000
    go.render()
    migrate_exports('correlation_matrix_cruise_track_modified')

# histogram()
# timeseries()
# gridded_map()
# sparse_map()
# contour_map()
# threeD()
# section_map()
# section_contour()
# depth_profile()
# cruise_track()
# correlation_matrix()
# correlation_matrix_cruise_track()
