import yaml
import numpy as np
import boto3
import os
import pandas as pd



def clean_data(df_dict, config):
    ENUM = 'Enum'
    PROPDEFINITIONS = 'PropDefinitions'
    TBD = 'TBD'
    NOT_REPORTED = 'not reported'
    with open(config['MODEL_FILE_PROPS']) as f:
        props = yaml.load(f, Loader = yaml.FullLoader)
    with open(config['CLEAN_DICT']) as f:
        clean_dict = yaml.load(f, Loader = yaml.FullLoader)
    for node, df in df_dict.items():
        for key in list(df.keys()):
            if key in list(props[PROPDEFINITIONS]):
                if ENUM in props[PROPDEFINITIONS][key]:
                    if props[PROPDEFINITIONS][key][ENUM][0] != TBD and props[PROPDEFINITIONS][key][ENUM][0] !=  NOT_REPORTED or len(props[PROPDEFINITIONS][key][ENUM]) > 1:
                        value_list = []
                        for value in df[key]:
                            if value not in props[PROPDEFINITIONS][key][ENUM]:
                                if key in clean_dict.keys():
                                    if value in clean_dict[key].keys():
                                        value_list.append(clean_dict[key][value])
                                        #print(clean_dict[key][value])
                                    elif value == np.nan and 'nan' in clean_dict[key].keys():
                                        value_list.append(clean_dict[key]['nan'])
                                    else:
                                        #value_list.append(None)
                                        value_list.append(value)
                                else:
                                    value_list.append(value)
                            else:
                                value_list.append(value)
                        df[key] = value_list
        df_dict[node] = df
    return df_dict
"""
                    if len(value_list) > 0:
                        print(key)
                        print(props[PROPDEFINITIONS][key][ENUM])
                        print(set(value_list))
"""

def upload_files(data_file, config, timestamp, cds_log):
    # Function to upload the transformed data to the s3 bucket
    # The subfolder name of the uploaded data will be timestamp
    # "data_file" is the path of the raw data files
    # "config" is the config file
    output_folder = os.path.basename(os.path.dirname(config['OUTPUT_FOLDER']))
    local_sub_folder_name_list = os.path.splitext(os.path.basename(data_file))
    local_sub_folder_name = local_sub_folder_name_list[0]
    s3 = boto3.client('s3')
    for file_name in os.listdir(os.path.join(config['OUTPUT_FOLDER'], local_sub_folder_name)):
        if file_name.endswith('.tsv'):
            # Find every file that end with '.tsv' and upload them to se bucket
            file_directory = os.path.join(config['OUTPUT_FOLDER'], local_sub_folder_name, file_name)
            s3_file_directory = os.path.join('Transformed', output_folder, timestamp, local_sub_folder_name, file_name)
            s3.upload_file(file_directory, config['S3_BUCKET'], s3_file_directory)
    subfolder = 's3://' + config['S3_BUCKET'] + '/' + 'Transformed' + '/' + output_folder + '/' + timestamp + '/' + local_sub_folder_name
    cds_log.info(f'Data files for {local_sub_folder_name} uploaded to {subfolder}')
    return timestamp

def print_data(df_dict, config, data_file, cds_log):
    # The function to store the transformed data to local file
    # The function will create a set of raw folders based on the name of the raw datafiles
    # "data_file" is the path of the raw data files
    sub_folder_name_list = os.path.splitext(os.path.basename(data_file))
    sub_folder_name = sub_folder_name_list[0]
    sub_folder = os.path.join(config['OUTPUT_FOLDER'], sub_folder_name)
    for file_name, df in df_dict.items():
        file_name = file_name + '.tsv'
        file_name = os.path.join(sub_folder, file_name)
        if not os.path.exists(sub_folder):
            os.makedirs(sub_folder)
        df_nulllist = list(df.isnull().all(axis=1))
        if False in df_nulllist:
            df.to_csv(file_name, sep = "\t", index = False)
            cds_log.info(f'Data node {os.path.basename(file_name)} is created and stored in {sub_folder}')


def combine_rows(df_dict, config):
    for combine_node in config['COMBINE_NODE']:
        combine_df = pd.DataFrame()
        df = df_dict[combine_node['node']]
        id_column = combine_node['id_column']
        for id in list(set(list(df[id_column]))):
            id_row = pd.DataFrame()
            for key in df.keys():
                values = df.loc[df[id_column] == id][key].dropna()
                value_list = list(set(values))
                if len(value_list) > 1:
                    value_string = ''
                    for i in range(0, len(value_list)):
                        if i != 0:
                            value_string = value_string + ',' + str(value_list[i])
                        else:
                            value_string = str(value_list[i])
                    id_row[key] = value_string
                else:
                    id_row[key] = value_list
            #print(id_row)
            combine_df = pd.concat([combine_df, id_row], ignore_index=True)
        df_dict[combine_node['node']] = combine_df
    return df_dict

def remove_node(df_dict, config):
    # The function to remove some extracted data node
    nodes = config['REMOVE_NODES']
    for node in nodes:
        df_dict.pop(node)
    return df_dict

def ui_validation(df_dict, config, data_file, cds_log):
    raw_data_name = os.path.basename(data_file)
    validation_df = pd.read_excel(io = config['VALIDATION_FILE'],
                                sheet_name =  "Must have properties",
                                engine = "openpyxl",
                                keep_default_na = False)
    for node in df_dict.keys():
        properties = list(validation_df.loc[validation_df['Node Name'] == node, 'Property Name'])
        if len(properties) > 0:
            for prop in properties:
                if prop not in df_dict[node].keys():
                    cds_log.warning('Data node {} does not have require UI property {} extracted from raw data file {}'.format(node, prop, raw_data_name))



