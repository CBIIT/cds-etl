import pandas as pd
import os
import yaml
import re
import argparse
from difflib import SequenceMatcher
import numpy as np
import glob
import dateutil.tz
import datetime
from cds_transformation_functions import clean_data, print_data, upload_files, combine_rows, remove_node, ui_validation, id_validation, download_from_s3, combine_columns, add_secondary_id, ssn_validation
from bento.common.utils import get_logger


cds_log = get_logger('CDS V1.3 Transformation Script')

def match_col(cds_df, property, limit):
    # The function to match the properties from the model file and the column name from the raw files
    # The function will create a list of candidate column names for each property name base on the similarity
    # The "limit" will decide the minimum similarity for the column name to be added into the candidate list
    col_list = []
    for col in cds_df.columns:
        col_name = col.replace(" ", "_").lower()
        s = SequenceMatcher(None)
        s.set_seq1(col_name)
        s.set_seq2(property)
        ratio = s.ratio()
        if ratio >= limit:
            col_list.append({'ratio': ratio, 'col': col})
    if len(col_list) == 0:
        return None
    max_index =  max(range(len(col_list)), key=lambda index: col_list[index]['ratio'])
    similar_col = col_list[max_index]['col']
    return similar_col

def extract_raw_data_dict(cds_df, model, node, limit, raw_dict):
    # The function to extract raw data dictionary from the raw data files
    #new_df = pd.DataFrame()
    # "cds_df" is the raw data data frame
    # "model" is the data model from the model file
    # "node" is the node name of the current node for extracting
    # "limit" is the minimum similarity
    # "raw_dict" is the raw data dictionary
    for property in model['Nodes'][node]['Props']:
        col = match_col(cds_df, property, limit)
        if col != None:
            if not cds_df[col].isnull().all():
                #new_df[property] = cds_df[col]
                if node not in raw_dict.keys():
                    raw_dict[node] = {}
                raw_dict[node][col] = property
    return raw_dict

def match_col_from_raw_dict(raw_dict, node, property, cds_df):
    # The function to match the column name from the raw files and the properties from the model file using raw data dictionary
    # "node" is the node name of the current node for transforming
    # "property" is the property from the model file
    # "raw_dict" is the raw data dictionary
    col_list = []
    if node in raw_dict.keys():
        for column, prop in raw_dict[node].items():
            if property == prop:
                col_list.append(column)
        for col in col_list:
            if col in cds_df.keys() and not cds_df[col].isnull().all():
                return col
    return None


def extract_data(cds_df, model, node, raw_data_dict):
    with open(raw_data_dict) as f:
        raw_dict = yaml.load(f, Loader = yaml.FullLoader)
    # The function to extract data from the raw data files
    new_df = pd.DataFrame()
    for property in model['Nodes'][node]['Props']:
        col = match_col_from_raw_dict(raw_dict, node, property, cds_df)
        if col != None:
            new_df[property] = cds_df[col]
    new_df_nulllist = list(new_df.isnull().all(axis=1))
    if False in new_df_nulllist:
        new_df['type'] = [node] * len(new_df)
    return new_df

def extract_parent_property(parent_mapping_column_list, df_dict):
    # Function to add parent id column to the data node files
    # "parent_mapping_column_list" is the parent relationship list from the config file
    # "df_dict" is the transformed data frame dictionary
    for node in df_dict.keys():
        new_df_nulllist = list(df_dict[node].isnull().all(axis=1))
        if False in new_df_nulllist:
            for parent_mapping_column in parent_mapping_column_list:
                    if node == parent_mapping_column['node']:
                        parent_mapping_column_name = parent_mapping_column['parent_node'] + '.' + parent_mapping_column['property']
                        if parent_mapping_column['property'] in df_dict[parent_mapping_column['parent_node']].keys():
                            df_dict[node][parent_mapping_column_name] = df_dict[parent_mapping_column['parent_node']][parent_mapping_column['property']]
    return df_dict


parser = argparse.ArgumentParser()
parser.add_argument('--config_file', type=str, help='The path of the config file.', required=True) #Argument about the config file
parser.add_argument('--upload_s3', help='Decide whether or not upload the transformed data to s3', action='store_true') #Argument to decide whether or not to upload the transformed data to the s3 bucket
parser.add_argument('--extract_raw_data_dictionary', help='Decide whether or not extract raw data dictionary instead of transformed raw data', action='store_true')
#parser.add_argument('--download_s3', help="Decide whether or not download datafiles from s3 bucket.", action='store_true')
args = parser.parse_args()
config = args.config_file
property_validation_df_columns = ['Missing_Properties', 'UI_Related', 'Raw_Data_File']
property_validation_df = pd.DataFrame(columns=property_validation_df_columns)
filename_validation_df_columns = ['Raw_Data_File', 'File_Name', 'Suspicious_SSN']
filename_validation_df = pd.DataFrame(columns=filename_validation_df_columns)

with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
ratio_limit = config['RATIO_LIMIT']
path = os.path.join(config['DATA_FOLDER'], config['DATA_BATCH_NAME'], '*.xlsx')
eastern = dateutil.tz.gettz('US/Eastern')
timestamp = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%dT%H%M%S")
#if args.download_s3 == True:
download_from_s3(config, cds_log)
if args.extract_raw_data_dictionary == False:
    for data_file in glob.glob(path):
        data_file_base = os.path.basename(data_file)
        #data_file_sheet_name = os.path.splitext(data_file_base)[0]
        cds_log.info(f'Start transforming {data_file_base}')
        # 'io' is the path of the excel file
        # 'sheet_name' is the sheet's name of the table we are going to read in
        # 'engine' is the engine used for reading in the data from excel
        # 'openpyxl' needs to install first and it is in the requirement.txt
        # 'keep_default_na' is whether or not to include the default NaN values when parsing the data.
        df_dict = {}
        Metadata = pd.read_excel(io = data_file,
                                sheet_name =  "Metadata",
                                engine = "openpyxl",
                                keep_default_na = False)
        # Replace all the empty string with NAN values
        Metadata = Metadata.replace(r'^\s*$', np.nan, regex=True)
        # Remove all leading and trailing spaces
        Metadata = Metadata.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        with open(config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for node in model['Nodes']:
            raw_data_dict = config['RAW_DATA_DICTIONARY']
            df_dict[node] = extract_data(Metadata, model, node, raw_data_dict)
        parent_mapping_column_list = config['PARENT_MAPPING_COLUMNS']
        df_dict = add_secondary_id(df_dict, config, cds_log)
        df_dict = combine_columns(df_dict, config, cds_log)
        df_dict = extract_parent_property(parent_mapping_column_list, df_dict)
        df_dict = remove_node(df_dict, config)
        for node in df_dict.keys():
            df_dict[node] = df_dict[node].drop_duplicates() #remove duplicate record
            df_nulllist = list(df_dict[node].isnull().all(axis=1))
            if False in df_nulllist:
                original_property_list = []
                for column_name in df_dict[node].keys():
                    if column_name in model['Nodes'][node]['Props']:
                        original_property_list.append(column_name)
                df_dict[node] = df_dict[node].dropna(subset = original_property_list, how='all')
        #df_dict = add_secondary_id(df_dict, config, cds_log)
        df_dict = combine_rows(df_dict, config, cds_log)
        df_dict = clean_data(df_dict, config)
        '''
        for index in range(0, len(df_dict['study'])):
            if 'experimental_strategy_and_data_subtypes' in df_dict['study'].keys():
                if df_dict['study']['experimental_strategy_and_data_subtypes'].notnull().any():
                    try:
                        es = re.split('\W+\s', str(df_dict['study']['experimental_strategy_and_data_subtypes'][index]))
                        df_dict['study']['experimental_strategy_and_data_subtypes'][index] = es
                    except Exception as e:
                        print(e)
        '''
        df_dict, property_validation_df = ui_validation(df_dict, config, data_file, cds_log, property_validation_df, model, data_file_base)
        filename_validation_df = ssn_validation(df_dict, data_file, cds_log, filename_validation_df)
        df_dict = id_validation(df_dict, config, data_file, cds_log)

        '''
        #check primary_diagnosis
        with open('b.txt', 'r') as file:
            b_file_contents = file.readlines()
            b_values_list = [value.strip() for value in b_file_contents]
        try:
            cds_log.info('start validating primary_diagnosis')
            p_list = []
            for i in df_dict['diagnosis']['primary_diagnosis']:
                if i not in b_values_list and i != 'Not specified in data':
                    p_list.append(i)
            p_list = list(set(p_list))
            cds_log.info(str(p_list))
        except:
            cds_log.info('no primary_diagnosis')

        '''
        #prefix = df_dict['study']['phs_accession'][0]
        prefix = os.path.splitext(data_file_base)[0]
        print_data(df_dict, config, cds_log, prefix)
    if args.upload_s3 == True:
        upload_files(config, timestamp, cds_log)

    sub_folder = os.path.join(config['ID_VALIDATION_RESULT_FOLDER'], config['DATA_BATCH_NAME'])
    property_validation_file_name = config['DATA_BATCH_NAME'] + '-' + 'Properties_validation_result' + '.tsv'
    property_validation_file_name = os.path.join(sub_folder, property_validation_file_name)
    filename_validation_file_name = config['DATA_BATCH_NAME'] + '-' + 'Filename_validation_result' + '.tsv'
    filename_validation_file_name = os.path.join(sub_folder,filename_validation_file_name)
    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)
    if len(property_validation_df) > 0:
        property_validation_df.to_csv(property_validation_file_name, sep = "\t", index = False)
        cds_log.info(f'Properties validation result file {os.path.basename(property_validation_file_name)} is created and stored in {sub_folder}')
    if len(filename_validation_df) > 0:
        filename_validation_df.to_csv(filename_validation_file_name, sep = "\t", index = False)
        cds_log.info(f'File name validation result file {os.path.basename(filename_validation_file_name)} is created and stored in {sub_folder}')

else:
    raw_dict = {}
    for data_file in glob.glob(path):
        data_file_base = os.path.basename(data_file)
        #data_file_sheet_name = os.path.splitext(data_file_base)[0]
        cds_log.info(f'Start extracting raw data dictionary from {data_file_base}')
        # 'io' is the path of the excel file
        # 'sheet_name' is the sheet's name of the table we are going to read in
        # 'engine' is the engine used for reading in the data from excel
        # 'openpyxl' needs to install first and it is in the requirement.txt
        # 'keep_default_na' is whether or not to include the default NaN values when parsing the data.
        Metadata = pd.read_excel(io = data_file,
                                sheet_name =  "Metadata",
                                engine = "openpyxl",
                                keep_default_na = False)
        # Replace all the empty string with NAN values
        Metadata = Metadata.replace(r'^\s*$', np.nan, regex=True)

        with open(config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for node in model['Nodes']:
            raw_dict = extract_raw_data_dict(Metadata, model, node, ratio_limit, raw_dict)
    with open(config['RAW_DATA_DICTIONARY'], 'w') as outfile:
        yaml.dump(raw_dict, outfile, default_flow_style=False)
    cds_log.info('Raw data dictionary is stored in {}'.format(config['RAW_DATA_DICTIONARY']))