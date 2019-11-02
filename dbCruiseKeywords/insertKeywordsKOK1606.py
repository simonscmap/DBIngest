import sys
import pycmap
sys.path.append('../')
import insertFunctions as iF
import config_vault as cfgv
import pandas as pd
sys.path.append('../dbCatalog/')
import catalogFunctions as cF
sys.path.append('../dbInsert/')
import insertPrep as ip
sys.path.append('../summary_stats/')
import summary_stats_func as ssf
import calendar
import geopandas as gpd
import shapely

sys.path.append('../login')
import credentials as cr
"""KOK1606_GRAD1 CRUISE KEYWORDS"""
cruise_name = 'KOK1606'
server = 'Rainier'
rawFilePath = cfgv.rep_cruise_keywords_raw
rawFileName = 'KOK1606_keywords.xlsx'
keyword_col = 'cruise_keywords'
# cruise_name = 'AMT13'





############################
""" Reads in the keyword excel file"""
df = pd.read_excel(rawFilePath + rawFileName)
"""saves over keyword excel file"""



prov_df = cF.getLonghurstProv(cruise_name)
ocean_df = cF.getOceanName(cruise_name)
seasons_df = cF.getCruiseSeasons(cruise_name)
months_df = cF.getCruiseMonths(cruise_name)
years_df = cF.getCruiseYear(cruise_name)
details_df = cF.getCruiseDetails(cruise_name)
# # long_name_df = cF.getCruiseAssosiatedLongName(cruise_name)
# #
# #
df = cF.addDFtoKeywordDF(df, prov_df)
df = cF.addDFtoKeywordDF(df, ocean_df)
df = cF.addDFtoKeywordDF(df, seasons_df)
df = cF.addDFtoKeywordDF(df, months_df)
df = cF.addDFtoKeywordDF(df, years_df)
df = cF.addDFtoKeywordDF(df, details_df)
# df = cF.addDFtoKeywordDF(df, long_name_df)
df = cF.removeDuplicates(df)
df = cF.stripWhitespace(df,keyword_col)
df = cF.removeAnyRedundantWord(df)


df.to_csv(rawFilePath + rawFileName + '_modified.csv',index=False)
