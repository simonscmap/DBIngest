import sys
import pycmap
sys.path.append('../')
import insertFunctions as iF
import config_vault as cfgv
import pandas as pd
sys.path.append('../dbCatalog/')
import catalogFunctions as cF


"""-----------------------------"""
"""KM1901 CRUISE KEYWORDS"""
"""-----------------------------"""

cruise_name = 'KM1901'
server = 'Rainier'
rawFilePath = cfgv.rep_cruise_keywords_raw
rawFileName = 'KM1901.xlsx'
keyword_col = 'cruise_keywords'


############################
""" Reads in the keyword excel file"""
df = pd.read_excel(rawFilePath + rawFileName)

ID = cF.getCruiseID(cruise_name)
prov_df = cF.getLonghurstProv(cruise_name)
ocean_df = cF.getOceanName(cruise_name)
seasons_df = cF.getCruiseSeasons(cruise_name)
months_df = cF.getCruiseMonths(cruise_name)
years_df = cF.getCruiseYear(cruise_name)
details_df = cF.getCruiseDetails(cruise_name)
short_name_df = cF.getCruiseAssosiatedShortName(cruise_name)
# long_name_df = cF.getCruiseAssosiatedLongName(cruise_name)
short_name_syn_df = cF.getShortNameSynonyms(cruise_name)

df = cF.addDFtoKeywordDF(df, short_name_syn_df)
df = cF.addDFtoKeywordDF(df, prov_df)
df = cF.addDFtoKeywordDF(df, ocean_df)
df = cF.addDFtoKeywordDF(df, seasons_df)
df = cF.addDFtoKeywordDF(df, months_df)
df = cF.addDFtoKeywordDF(df, years_df)
df = cF.addDFtoKeywordDF(df, details_df)
df = cF.addDFtoKeywordDF(df, short_name_df)
# df = cF.addDFtoKeywordDF(df, long_name_df)
df = cF.removeDuplicates(df)
df = cF.stripWhitespace(df,keyword_col)
df = cF.removeAnyRedundantWord(df)

""" INSERTS INTO tblCruise_Keywords"""
cF.insertCruiseKeywords(ID,df,server)
