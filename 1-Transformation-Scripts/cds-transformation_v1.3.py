import pandas as pd
import os
import yaml
import argparse
from difflib import SequenceMatcher
import numpy as np
import glob
import dateutil.tz
import datetime
from cds_transformation_functions import clean_data, print_data, upload_files, combine_rows, remove_node, ui_validation
import random
from bento.common.utils import get_logger
import uuid

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
    col_list = []
    if node in raw_dict.keys():
        for column, prop in raw_dict[node].items():
            if property == prop:
                col_list.append(column)
        for col in col_list:
            if col in cds_df.keys() and not cds_df[col].isnull().all():
                return col
    return None


def extract_data(cds_df, model, node, raw_data_dict, node_id_field_list):
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
        # If the extracted dataframe not only consist with NAN values
        # Then add the 'type' property to the dataframe and add the parent mapping column
        #add file ID column
        for node_id_field in node_id_field_list:
            if node == node_id_field['node']:
                if node_id_field['id_field'] not in new_df.keys():
                    if 'backup_id_field' in node_id_field.keys():
                        find_backup_id_field = False
                        for backup_id_field in node_id_field['backup_id_field']:
                            if backup_id_field in cds_df.keys():
                                new_df[node_id_field['id_field']] = cds_df[backup_id_field]
                                find_backup_id_field = True
                        if not find_backup_id_field:
                            id_column = id_column = list((uuid.uuid4() for x in range(len(new_df))))
                            new_df[node_id_field['id_field']] = id_column
                    else:
                        id_column = list((uuid.uuid4() for x in range(len(new_df))))
                        new_df[node_id_field['id_field']] = id_column
        new_df['type'] = [node] * len(new_df)
    return new_df

def extract_parent_property(parent_mapping_column_list, df_dict):
    for node in df_dict.keys():
        new_df_nulllist = list(df_dict[node].isnull().all(axis=1))
        if False in new_df_nulllist:
            for parent_mapping_column in parent_mapping_column_list:
                    if node == parent_mapping_column['node']:
                        parent_mapping_column_name = parent_mapping_column['parent_node'] + '.' + parent_mapping_column['property']
                        if parent_mapping_column['property'] in df_dict[parent_mapping_column['parent_node']].keys():
                            df_dict[node][parent_mapping_column_name] = df_dict[parent_mapping_column['parent_node']][parent_mapping_column['property']]
    return df_dict

def add_id_fields_after(df_dict, config, cds_df):
    node_id_field_list = config['NODE_ID_FIELD_AFTER']
    for node in df_dict.keys():
        for node_id_field in node_id_field_list:
                if node == node_id_field['node']:
                    if node_id_field['id_field'] not in df_dict[node].keys():
                        if 'backup_id_field' in node_id_field.keys():
                            find_backup_id_field = False
                            for backup_id_field in node_id_field['backup_id_field']:
                                if backup_id_field in cds_df.keys():
                                    df_dict[node][node_id_field['id_field']] = cds_df[backup_id_field]
                                    find_backup_id_field = True
                            if not find_backup_id_field:
                                id_column = id_column = list((uuid.uuid4() for x in range(len(df_dict[node]))))
                                df_dict[node][node_id_field['id_field']] = id_column
                        else:
                            id_column = list((uuid.uuid4() for x in range(len(df_dict[node]))))
                            df_dict[node][node_id_field['id_field']] = id_column
    return df_dict


parser = argparse.ArgumentParser()
parser.add_argument('--config_file', type=str, help='The path of the config file.', required=True) #Argument about the config file
parser.add_argument('--upload_s3', help='Decide whether or not upload the transformed data to s3', action='store_true')
parser.add_argument('--extract_raw_data_dictionary', help='Decide whether or not extract raw data dictionary instead of transformed raw data', action='store_true')
args = parser.parse_args()
config = args.config_file

with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
ratio_limit = config['RATIO_LIMIT']
path = os.path.join(config['DATA_FOLDER'], '*.xlsx')
eastern = dateutil.tz.gettz('US/Eastern')
timestamp = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%dT%H%M%S")
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

        with open(config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for node in model['Nodes']:
            raw_data_dict = config['RAW_DATA_DICTIONARY']
            node_id_field_list = config['NODE_ID_FIELD_BEFORE']
            df_dict[node] = extract_data(Metadata, model, node, raw_data_dict, node_id_field_list)
        parent_mapping_column_list = config['PARENT_MAPPING_COLUMNS']
        df_dict = extract_parent_property(parent_mapping_column_list, df_dict)
        df_dict = remove_node(df_dict, config)
        for node in df_dict.keys():
            df_dict[node] = df_dict[node].drop_duplicates() #remove duplicate record
        df_dict = add_id_fields_after(df_dict, config, Metadata)
        df_dict = combine_rows(df_dict, config)
        df_dict = clean_data(df_dict, config)
        ui_validation(df_dict, config, data_file, cds_log)
        print_data(df_dict, config, data_file, cds_log)
        if args.upload_s3 == True:
            upload_files(data_file, config, timestamp, cds_log)
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