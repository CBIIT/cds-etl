import yaml
import numpy as np
import boto3
import os
import pandas as pd



def clean_data(df, config):
    ENUM = 'Enum'
    PROPDEFINITIONS = 'PropDefinitions'
    TBD = 'TBD'
    NOT_REPORTED = 'not reported'
    with open(config['MODEL_FILE_PROPS']) as f:
        props = yaml.load(f, Loader = yaml.FullLoader)
    with open(config['CLEAN_DICT']) as f:
        clean_dict = yaml.load(f, Loader = yaml.FullLoader)
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
                                elif value == np.nan:
                                    value_list.append(clean_dict[key]['nan'])
                                else:
                                    value_list.append(None)
                            else:
                                value_list.append(value)
                        else:
                            value_list.append(value)
                    df[key] = value_list
    return df
"""
                    if len(value_list) > 0:
                        print(key)
                        print(props[PROPDEFINITIONS][key][ENUM])
                        print(set(value_list))
"""

def upload_files(data_file, config, timestamp):
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
    print(f'Data files for {local_sub_folder_name} uploaded to {subfolder}')
    return timestamp

def print_data(df, config, file_name, data_file):
    # The function to store the transformed data to local file
    # The function will create a set of raw folders based on the name of the raw datafiles
    # "file_name" is the data node's name
    # "data_file" is the path of the raw data files
    sub_folder_name_list = os.path.splitext(os.path.basename(data_file))
    sub_folder_name = sub_folder_name_list[0]
    sub_folder = os.path.join(config['OUTPUT_FOLDER'], sub_folder_name)
    file_name = file_name + '.tsv'
    file_name = os.path.join(sub_folder, file_name)
    if not os.path.exists(sub_folder):
       os.makedirs(sub_folder)
    df_nulllist = list(df.isnull().all(axis=1))
    if False in df_nulllist:
        df.to_csv(file_name, sep = "\t", index = False)
        print(f'Data node {os.path.basename(file_name)} is created and stored in {sub_folder}')


def combine_rows(dict, config):
    for combine_node in config['COMBINE_NODE']:
        combine_df = pd.DataFrame()
        df = dict[combine_node['node']]
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
        dict[combine_node['node']] = combine_df
    return dict

def remove_node(dict, config):
    # The function to remove some extracted data node
    nodes = config['REMOVE_NODES']
    for node in nodes:
        dict.pop(node)
    return dict




