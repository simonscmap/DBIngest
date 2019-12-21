import pandas as pd
import pprint
import datetime as dt
import numpy as np
import os
import re
import time


class pycmap_dataset_metadata():

    """
    CASES TO CHECK FOR:

    1. file is excel or csv
    2. Columns are: [dataset_short_name,dataset_long_name,dataset_version,dataset_release_date,dataset_make,dataset_doi,dataset_description,dataset_references]
    3. col, dataset_short_name - No spaces, no special characters, character limit of ??
    4. col, dataset_lon_name - Limit, suggested limit - if name over, print example
    5. col dataset_version - exists?, char limit
    6. col dataset_release_date - exists?, char limit  - Do we want this?
    7. col dataset_make- exists, if exists in: ['Observation', 'Model', 'Assimilation']
    8. col dataset_DOI - exists, format like https://doi.org/......? Suggest, link to zenodo etc
    9. col dataset_description - use pprint to show description. How does this look?
    10. col dataset_references - list of refs.
    """

    """
    Validates dataset metadata for Simons CMAP submission

    Parameters
    ----------
    csv_path : string
        String of path to metadata csv
    """


    def __init__(self, path):
        self.path = path
        self.ftype = None
        self.df = None


        self.fpath_checker()
        self.file_type()
        self.import_file()
        #
        #
        # self.print_col_spacer('Checking that all dataset_metadata columns exist')
        # self.check_cols()
        # self.print_col_spacer('Checking dataset_short_names')
        # self.check_short_name()
        # self.print_col_spacer('Checking dataset_long_names')
        # self.check_long_name()
        # self.print_col_spacer('Checking dataset_version')
        # self.check_dataset_version()
        # self.print_col_spacer('Checking dataset_release_date')
        # self.check_dataset_release_date()
        # self.print_col_spacer('Checking dataset_make')
        # self.check_dataset_make()
        # self.print_col_spacer('Checking dataset_DOI')
        # self.check_dataset_DOI()
        # self.print_col_spacer('Checking dataset_description')
        # self.check_dataset_description()
        # self.print_col_spacer('Checking dataset_references')
        # self.check_dataset_references()
        # self.print_col_spacer('')
        # print('Thank you use using the dataset validator. If there were any warnings, please modify your data and re-run the validator. For more detailed questions please join our Slack channel or email nrhagen@uw.edu. Data submission guidelines can be found here: https://cmap.readthedocs.io/en/latest/faq_and_contributing/datasubmission.html')


    def fpath_checker(self):
        if isinstance(self.path,str) is False:
            raise ValueError("Supplied path doesn't seem to be a string")

    def file_type(self):
        if os.path.basename(self.path).split('.')[1] == 'csv':
            self.ftype = 'csv'
        elif os.path.basename(self.path).split('.')[1] == 'xlsx':
            self.ftype = 'excel'
        else:
            print('Metadata file type does not appear to be a csv or excel file')


    def import_file(self):
        if self.ftype == 'csv':
            df = pd.read_csv(self.path)
        elif self.ftype == 'excel':
            df = pd.read_excel(self.path)
        else:
            print('File type not recognized')
            sys.exit()
        df = df.replace(np.nan, '', regex=True)
        self.df = df

    def print_col_spacer(self,col_name):
        print('\n')
        # input("Press any key to continue...")
        # print('\n')
        print('-- {}'.format(str(col_name)))
        print('\n')
        # time.sleep(1)

    def length_validator(self,string, length):
        if len(string) > int(length):
            raise ValueError(string, ' is longer then.'+ length + '. Please modify it to be below this limit.')

    def length_validator_long_name(self,string):
        if len(string) > 150:
            print('The dataset_long_name provided was: ' + str(string) + '\n')
            print('A dataset_long_name should be a human-readable descriptive name. This is not a dataset description and should be kept relativly short.'  + '\n' +
                'Examples include: ' + '\n' +
                'MODIS Aerosol Optical Depth (Reprocessed) ' + '\n' +
                'Darwin Biogeochemistry 3 Day Averaged Model of Bulk Ecosystem Characteristics' + '\n' +
                'Present and future global distributions of the marine Cyanobacteria Prochlorococcus and Synechococcus (Flombaum)')
        if len(string) > int(500):
            raise ValueError('The dataset_long_name is longer then.'+ str(500) + '. Please modify it to be below this limit.')
    def space_validator(self,string):
        if ' ' in string:
            raise ValueError(string, ' contains spaces. Please modify to remove spaces.')


    def check_cols(self):
        self.print_col_spacer('Checking that all dataset_metadata columns exist')
        col_list = ['dataset_short_name','dataset_long_name','dataset_version','dataset_release_date','dataset_make','dataset_doi','dataset_description','dataset_references']
        if self.df.columns.to_list() != col_list:
            print('Columns do not seem to be: ','\n', col_list,'\n', self.df.columns.to_list())

    """col, dataset_short_name - No spaces, character limit of ??"""
    def check_short_name(self):
        self.space_validator(self.df['dataset_short_name'][0])
        self.length_validator(self.df['dataset_short_name'][0], 100)
        print('Does the dataset_short_name: [' + self.df['dataset_short_name'][0] + '] seem correct?')

    """ col, dataset_lon_name - Limit, suggested limit - if name over, print example"""
    def check_long_name(self):
        self.length_validator_long_name(self.df['dataset_long_name'][0])
        print('Does the dataset_long_name: [' + self.df['dataset_long_name'][0] + '] seem correct?')

    """     5. col dataset_version - exists?, char limit"""
    def check_dataset_version(self):
        if len(str(self.df['dataset_version'][0])) < 1:
            raise ValueError('Dataset version: ' + str(self.df['dataset_version'][0]) + ' appears to be missing. Please add a version. Examples include: ' + '\n' +
            'V1, Version 1, Version 1.02, Initial, Preliminary etc.')
        print('Does the dataset_version: [' + self.df['dataset_version'][0] + '] seem correct?')

    """     6. col dataset_release_date - exists?, char limit  - Do we want this?"""
    def check_dataset_release_date(self):
        if len(str(self.df['dataset_release_date'][0])) < 1:
            raise ValueError('Dataset release date: ' + str(self.df['dataset_release_date'][0]) + ' appears to be missing. Please add a release date.')
        print('Does the dataset_release_date: [' + self.df['dataset_release_date'][0] + '] seem correct?')

    """     7. col dataset_make- exists, if exists in: ['Observation', 'Model', 'Assimilation']"""
    def check_dataset_make(self):
        if str(self.df['dataset_make'][0]).lower() not in ['observation', 'model', 'assimilation']:
            raise ValueError('Dataset Make: ' + str(self.df['check_dataset_make'][0]) + ' is not observation, model or assimilation. Please categorize your dataset make.')
        print('Does the dataset_make: [' + self.df['dataset_make'][0] + '] seem correct?')

    """    8. col dataset_DOI - exists, format like https://doi.org/......? Suggest, link to zenodo etc"""
    def check_dataset_DOI(self):
        if len(str(self.df['dataset_doi'][0])) < 1:
            raise ValueError('Dataset DOI: ' + str(self.df['dataset_doi'][0]) + ' appears to be missing. Please add a DOI (Digital Object Identifier). A DOI allows users to cite the use of a dataset and properly acknowledge the dataset creators. A DOI can be created for free with multiple services such as Zenodo, Dryad or Figshare. Please see https://cmap.readthedocs.io/en/latest/faq_and_contributing/FAQ.html for more information.')
        print('Does the dataset_doi: [' + self.df['dataset_doi'][0] + '] seem correct?')

    """   9. col dataset_description - use pprint to show description. How does this look?"""
    def check_dataset_description(self):
        if len(str(self.df['dataset_description'][0])) < 1:
            raise ValueError('Dataset Desciption: ' + str(self.df['dataset_doi'][0]) + ' appears to be missing. Please add a detailed description of your dataset. Examples of other dataset desctions can be found at: https://cmap.readthedocs.io/en/latest/catalog/catalog.html.')
        print('Does this dataset_description seem correct?: ' + self.df['dataset_description'][0])

    """   10. col dataset_references"""
    def check_dataset_references(self):
        if len(self.df['dataset_references']) < 1:
            raise ValueError('Dataset References appears to be missing. Please add a any references or links you wish.')
        elif len(self.df['dataset_references']) >= 1:
            dataset_ref_list = self.df['dataset_references'].tolist()
            print('Do these references look correct?')
            print('\n')
            for ref in dataset_ref_list:
                print(ref)
                print('\n')

    # print_col_spacer('Checking that all dataset_metadata columns exist')
    # check_cols(self)
    # print_col_spacer('Checking dataset_short_names')
    # check_short_name(self)
    # print_col_spacer('Checking dataset_long_names')
    # check_long_name(self)
    # print_col_spacer('Checking dataset_version')
    # check_dataset_version(self)
    # print_col_spacer('Checking dataset_release_date')
    # check_dataset_release_date(self)
    # print_col_spacer('Checking dataset_make')
    # check_dataset_make(self)
    # print_col_spacer('Checking dataset_DOI')
    # check_dataset_DOI(self)
    # print_col_spacer('Checking dataset_description')
    # check_dataset_description(self)
    # print_col_spacer('Checking dataset_references')
    # check_dataset_references(self)
    # print_col_spacer('')
    # print('Thank you use using the dataset validator. If there were any warnings, please modify your data and re-run the validator. For more detailed questions please join our Slack channel or email nrhagen@uw.edu. Data submission guidelines can be found here: https://cmap.readthedocs.io/en/latest/faq_and_contributing/datasubmission.html')
    #




df =pycmap_dataset_metadata('dataset_metadata_test.csv')
