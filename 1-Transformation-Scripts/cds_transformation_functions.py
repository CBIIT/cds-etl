import yaml
import numpy as np
import boto3
import os
import pandas as pd



def clean_data(df_dict, config):
    # The function to clean the transformed data base on the clean data dictionary
    # "df_dict" is the transformed data frame dictionary
    # "config" is the config file
    # The function will only replace the property's value if the property have "Enum" list value specific in the model props file
    # and the "Enum" list value does not have only one "TBD" or "NOT_REPORTED" value inside the list
    # and the property's value is not inside the "Enum" list but is record in the raw data dictionary
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
                                    elif pd.isnull(value) and 'nan_value' in clean_dict[key].keys():
                                        value_list.append(clean_dict[key]['nan_value'])
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

def upload_files(config, timestamp, cds_log):
    # Function to upload the transformed data to the s3 bucket
    # The subfolder name of the uploaded data will be timestamp
    # "data_file" is the path of the raw data files
    # "config" is the config file
    local_sub_folder_name = config['DATA_BATCH_NAME']
    s3 = boto3.client('s3')
    for file_name in os.listdir(os.path.join(config['OUTPUT_FOLDER'], local_sub_folder_name)):
        if file_name.endswith('.tsv'):
            # Find every file that end with '.tsv' and upload them to se bucket
            file_directory = os.path.join(config['OUTPUT_FOLDER'], local_sub_folder_name, file_name)
            s3_file_directory = os.path.join('transformed', config['DATA_BATCH_NAME'], timestamp, file_name)
            s3.upload_file(file_directory, config['S3_BUCKET'], s3_file_directory)
    for file_name in os.listdir(os.path.join(config['DATA_FOLDER'], local_sub_folder_name)):
        if file_name.endswith('.xlsx'):
            # Find every file that end with '.tsv' and upload them to se bucket
            file_directory = os.path.join(config['DATA_FOLDER'], local_sub_folder_name, file_name)
            s3_file_directory = os.path.join('raw', config['DATA_BATCH_NAME'], timestamp, file_name)
            s3.upload_file(file_directory, config['S3_BUCKET'], s3_file_directory)
    transformed_subfolder = 's3://' + config['S3_BUCKET'] + '/' + 'transformed' + '/' + config['DATA_BATCH_NAME'] + '/' + timestamp
    raw_subfolder = 's3://' + config['S3_BUCKET'] + '/' + 'raw' + '/' + config['DATA_BATCH_NAME'] + '/' + timestamp
    cds_log.info(f'Transformed data files for {local_sub_folder_name} uploaded to {transformed_subfolder}')
    cds_log.info(f'Raw data files for {local_sub_folder_name} uploaded to {raw_subfolder}')

def print_data(df_dict, config, cds_log, prefix):
    # The function to store the transformed data to local file
    # The function will create a set of raw folders based on the name of the raw datafiles
    # "data_file" is the path of the raw data files
    # "config" is the config file
    # "prefix" is the prefix of transfomed files
    # "cds_log" is the log object
    sub_folder = os.path.join(config['OUTPUT_FOLDER'], config['DATA_BATCH_NAME'])
    for file_name, df in df_dict.items():
        file_name = prefix + '-' + file_name + '.tsv'
        file_name = os.path.join(sub_folder, file_name)
        if not os.path.exists(sub_folder):
            os.makedirs(sub_folder)
        df_nulllist = list(df.isnull().all(axis=1))
        if False in df_nulllist:
            df.to_csv(file_name, sep = "\t", index = False)
            cds_log.info(f'Data node {os.path.basename(file_name)} is created and stored in {sub_folder}')


def combine_rows(df_dict, config, cds_log):
    # The function to combine rows base on the config file
    # "df_dict" is the transformed data frame dictionary
    # "config" is the config file
    for combine_node in config['COMBINE_NODE']:
        try:
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
                            value_item = str(value_list[i]).strip()
                            if i != 0:
                                if value_item not in value_string:
                                    value_string = value_string + ', ' + value_item
                            else:
                                value_string = value_item
                        id_row[key] = [value_string]
                    elif len(value_list) == 1:
                        id_row[key] = value_list
                    else:
                        id_row[key] = [np.nan]
                #print(id_row)
                combine_df = pd.concat([combine_df, id_row], ignore_index=True)
            df_dict[combine_node['node']] = combine_df
        except Exception as e:
            cds_log.warning('Data node {} dose not exist'.format(combine_node))
            cds_log.error(e)
    return df_dict

def remove_node(df_dict, config):
    # The function to remove some extracted data node
    # "data_file" is the path of the raw data files
    # "config" is the config file
    nodes = config['REMOVE_NODES']
    for node in nodes:
        df_dict.pop(node)
    return df_dict

def delete_children(parent_mapping_column_list, delete_list, parent_node, df_dict, config):
    for parent_mapping_column in parent_mapping_column_list:
        if parent_mapping_column['parent_node'] == parent_node and len(df_dict[parent_mapping_column['node']]) > 0:
            parent_id_field = parent_mapping_column['parent_node'] + '.' + parent_mapping_column['property']
            children_node = parent_mapping_column['node']
            for pc in parent_mapping_column_list:
                if pc['parent_node'] == children_node and len(df_dict[pc['node']]) > 0:
                    #pc_children_node = pc['node']
                    children_delete_list_df = df_dict[children_node][df_dict[children_node][parent_id_field ].isin(delete_list)]
                    children_delete_list = list(children_delete_list_df[config['NODE_ID_FIELD'][children_node]])
                    df_dict = delete_children(parent_mapping_column_list, children_delete_list, children_node, df_dict, config)
            df_dict[children_node] = df_dict[children_node][~df_dict[children_node][parent_id_field].isin(delete_list)]
    return df_dict

def print_id_validation_result(id_validation_df, config, cds_log, prefix):
    sub_folder = os.path.join(config['ID_VALIDATION_RESULT_FOLDER'], config['DATA_BATCH_NAME'])
    file_name = prefix + '-' + 'ID_validation_result' + '.tsv'
    file_name = os.path.join(sub_folder, file_name)
    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)
    id_validation_df.to_csv(file_name, sep = "\t", index = False)
    cds_log.info(f'ID data validation result file {os.path.basename(file_name)} is created and stored in {sub_folder}')

def id_validation(df_dict, config, data_file, cds_log):
    id_validation_df = pd.DataFrame(columns = ['node name', 'ID', 'conflict property'])
    raw_data_name = os.path.basename(data_file)
    parent_mapping_column_list = config['PARENT_MAPPING_COLUMNS']
    for node in df_dict.keys():
        if len(df_dict[node]) > 0:
            df_dict[node] = df_dict[node].dropna(subset = [config['NODE_ID_FIELD'][node]])
            for parent_column in config['PARENT_MAPPING_COLUMNS']:
                if parent_column['node'] == node:
                    parent_id_field = parent_column['parent_node'] + '.' + parent_column['property']
                    if parent_id_field in df_dict[node].keys():
                        df_dict[node] = df_dict[node].dropna(subset = [parent_id_field])
            if node in config['NODE_ID_FIELD'].keys():
                id_validation_result = [x for x in set(list(df_dict[node][config['NODE_ID_FIELD'][node]])) if list(df_dict[node][config['NODE_ID_FIELD'][node]]).count(x) > 1]
                if len(id_validation_result) > 0:
                    cds_log.warning('The ID {} is duplicate in the node {} from the study {}'.format(id_validation_result, node, raw_data_name))
                    cds_log.warning('Removed all data related to the duplicated ID {} from the node {} from the study {}'.format(id_validation_result, node, raw_data_name))
                    deleted_record = df_dict[node][df_dict[node][config['NODE_ID_FIELD'][node]].isin(id_validation_result)]
                    conflicted_column_names = []
                    for column_name in deleted_record.keys():
                        deleted_id_list = set(list(deleted_record[config['NODE_ID_FIELD'][node]]))
                        conflicted_column = False
                        for id in deleted_id_list:
                            deleted_id_df = deleted_record.loc[deleted_record[config['NODE_ID_FIELD'][node]] == id]
                            if len(set(list(deleted_id_df[column_name]))) > 1:
                                conflicted_column = True
                        #for id in deleted_id_list
                        if conflicted_column and column_name != config['NODE_ID_FIELD'][node]:
                            conflicted_column_names.append(column_name)
                    for deleted_record_ID in set(list(deleted_record[config['NODE_ID_FIELD'][node]])):
                        id_validation_df_row = pd.DataFrame(data = [[node, deleted_record_ID, conflicted_column_names]], columns = ['node name', 'ID', 'conflict property'])
                        id_validation_df = pd.concat([id_validation_df, id_validation_df_row], ignore_index=True)
                    df_dict[node] = df_dict[node][~df_dict[node][config['NODE_ID_FIELD'][node]].isin(id_validation_result)]
                    df_dict = delete_children(parent_mapping_column_list, id_validation_result, node, df_dict, config)
                    """
                    for parent_mapping_column in parent_mapping_column_list:
                        if parent_mapping_column['parent_node'] == node and len(df_dict[parent_mapping_column['node']]) > 0:
                            parent_id_field = parent_mapping_column['parent_node'] + '.' + parent_mapping_column['property']
                            children_node = parent_mapping_column['node']
                            df_dict[children_node] = df_dict[children_node][~df_dict[children_node][parent_id_field].isin(id_validation_result)]
                    """
    if len(id_validation_df) > 0:
        #prefix = df_dict['study']['phs_accession'][0]
        prefix = os.path.splitext(os.path.basename(data_file))[0]
        print_id_validation_result(id_validation_df, config, cds_log, prefix)
    return df_dict

def ui_validation(df_dict, config, data_file, cds_log):
    # The function to do check if the UI related properties are in the transformed data files
    # "data_file" is the path of the raw data files
    # "config" is the config file
    # "cds_log" is the log object
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
                    df_dict[node][prop] = ['Not specified in data'] * len(df_dict[node])
                    cds_log.warning('The data node {} does not have require UI property {} extracted from raw data file {}'.format(node, prop, raw_data_name))
                elif df_dict[node][prop].isnull().values.any():
                    df_dict[node][prop] = df_dict[node][prop].replace(np.nan, 'Not specified in data')
    return df_dict

def download_from_s3(config, cds_log):
        # Function to download raw data files from the s3 bucket
        # The user can decide use this function to get raw data or just read raw data from local
        # 's3' is a boto3 s3 object
        s3 = boto3.client('s3')
        subfolder_directory = config['S3_RAWDATA_SUBFOLDER']
        cds_log.info('Start downloading file from s3 {}'.format(os.path.join(config['S3_BUCKET'], subfolder_directory)))
        for key in s3.list_objects(Bucket = config['S3_BUCKET'], Prefix = subfolder_directory)['Contents']:
            download_folder = os.path.join(config['DATA_FOLDER'], config['DATA_BATCH_NAME'])
            if not os.path.exists(download_folder):
                # If the path does not exist, then create the folder
                os.mkdir(download_folder)
            file_key = os.path.join(download_folder, os.path.basename(key['Key']))
            s3.download_file(config['S3_BUCKET'], key['Key'], file_key)

def combine_columns(df_dict, config, cds_log):
    for combine_node in config['COMBINE_COLUMN']:
        if combine_node['node'] in df_dict.keys():
            try:
                df_dict[combine_node['node']][combine_node['new_column']] = df_dict[combine_node['node']][combine_node['column1']].astype(str) + "_" + df_dict[combine_node['node']][combine_node['column2']].astype(str)
            except Exception as e:
                cds_log.error(e)
    return df_dict

def add_secondary_id(df_dict, config, cds_log):
    for secondary_id_node in config['SECONDARY_ID_COLUMN']:
        if secondary_id_node['node'] in df_dict.keys():
            df_nulllist = list(df_dict[secondary_id_node['node']].isnull().all(axis=1))
            if False in df_nulllist:
                if secondary_id_node['node_id'] not in df_dict[secondary_id_node['node']].keys():
                    cds_log.warning('The ID {} is missing and will be replaced by {} for the node {}'.format(secondary_id_node['node_id'], secondary_id_node['secondary_id'], secondary_id_node['node']))
                    df_dict[secondary_id_node['node']][secondary_id_node['node_id']] = df_dict[secondary_id_node['node']][secondary_id_node['secondary_id']]
    return df_dict