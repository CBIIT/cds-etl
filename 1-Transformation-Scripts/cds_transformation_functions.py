import yaml
import numpy as np
import boto3
import os
import pandas as pd
import re
import sys
import glob

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
        props = yaml.safe_load(f)
    with open(config['CLEAN_DICT']) as f:
        clean_dict = yaml.safe_load(f)
    for node, df in df_dict.items():
        df.reset_index(inplace = True, drop=True)
        for key in list(df.keys()):
            if key in list(props[PROPDEFINITIONS]):
                if ENUM in props[PROPDEFINITIONS][key]:
                    if props[PROPDEFINITIONS][key][ENUM][0] != TBD and props[PROPDEFINITIONS][key][ENUM][0] !=  NOT_REPORTED or len(props[PROPDEFINITIONS][key][ENUM]) > 1:
                        value_list = []
                        for value in df[key]:
                            if value not in props[PROPDEFINITIONS][key][ENUM]:
                                if key in clean_dict.keys():
                                    str_value = str(value)
                                    #print(len(list(clean_dict['primary_diagnosis'].keys())))
                                    if str_value in clean_dict[key].keys() or value in clean_dict[key].keys():
                                        try:
                                            value_list.append(clean_dict[key][value])
                                        except:
                                            value_list.append(clean_dict[str(key)][str_value])
                                        #print(clean_dict[key][value])
                                    elif pd.isnull(value) and 'nan_value' in clean_dict[key].keys():
                                        value_list.append(clean_dict[key]['nan_value'])
                                    elif value in clean_dict["extra_long_values"]: #if the value is too long to be the key of a yaml file
                                        #print(value)
                                        value_list.append("Not specified in data")
                                    else:
                                        #value_list.append(None)
                                        value_list.append(value)
                                else:
                                    value_list.append(value)
                            else:
                                value_list.append(value)
                        df[key] = value_list
                elif "Type" in props[PROPDEFINITIONS][key]:
                    if props[PROPDEFINITIONS][key]["Type"] == 'integer':
                        value_list = []
                        for value in df[key]:
                            if not isinstance(value, int) and value is not None and not pd.isna(value):
                                try:
                                    int_value = int(value)
                                    if value == int_value: 
                                        value_list.append(int_value)
                                    else:
                                        value_list.append(value)
                                except Exception as e:
                                    value_list.append(value)
                                    print(e)             
                            else:
                                value_list.append(value)
                        df[key] = pd.Series(value_list, dtype=object)
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
                    value_list.sort()
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

def get_parent_list(node, parent_mapping_column_list):
    parent_list = []
    for parent_mapping_column in parent_mapping_column_list:
        if node == parent_mapping_column['node']:
            parent_list.append(parent_mapping_column['parent_node'] + '.' + parent_mapping_column['property'])
    return parent_list

def delete_children(parent_mapping_column_list, delete_list, parent_node, df_dict, config):
    for parent_mapping_column in parent_mapping_column_list:
        if parent_mapping_column['parent_node'] == parent_node and len(df_dict[parent_mapping_column['node']]) > 0:
            parent_id_field = parent_mapping_column['parent_node'] + '.' + parent_mapping_column['property']
            children_node = parent_mapping_column['node']
            parent_list = get_parent_list(children_node, parent_mapping_column_list)
            df_dict[children_node].loc[df_dict[children_node][parent_id_field].isin(delete_list), parent_id_field] = np.nan #replace the parent id with nan values
            for pc in parent_mapping_column_list:
                if pc['parent_node'] == children_node and len(df_dict[pc['node']]) > 0:
                    #pc_children_node = pc['node']
                    #children_delete_list_df = df_dict[children_node][df_dict[children_node][parent_id_field].isna()]
                    children_delete_list_df = df_dict[children_node][df_dict[children_node][parent_list].isna().all(axis=1)]
                    children_delete_list = list(children_delete_list_df[config['NODE_ID_FIELD'][children_node]])
                    df_dict = delete_children(parent_mapping_column_list, children_delete_list, children_node, df_dict, config)

            df_dict[children_node] = df_dict[children_node].dropna(subset = parent_list, how='all')
            #df_dict[children_node] = df_dict[children_node][~df_dict[children_node][parent_id_field].isin(delete_list)]
    return df_dict

def print_id_validation_result(id_validation_df, config, cds_log, prefix, parent):
    sub_folder = os.path.join(config['ID_VALIDATION_RESULT_FOLDER'], config['DATA_BATCH_NAME'])
    if parent:
        file_name = prefix + '-' + 'parent_ID_validation_result' + '.tsv'
    else:
        file_name = prefix + '-' + 'ID_validation_result' + '.tsv'
    file_name = os.path.join(sub_folder, file_name)
    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)
    id_validation_df.to_csv(file_name, sep = "\t", index = False)
    cds_log.info(f'ID data validation result file {os.path.basename(file_name)} is created and stored in {sub_folder}')

def id_validation(df_dict, config, data_file, cds_log, model):
    id_validation_df = pd.DataFrame(columns = ['node name', 'ID', 'conflict property'])
    parent_id_validation_df = pd.DataFrame(columns = ['node name', 'ID', 'parent ID field'])
    prefix = os.path.splitext(os.path.basename(data_file))[0]
    raw_data_name = os.path.basename(data_file)
    mul = "many_to_one"
    parent_mapping_column_list = config['PARENT_MAPPING_COLUMNS']
    for node in df_dict.keys():
        if len(df_dict[node]) > 0:
            df_dict[node] = df_dict[node].drop_duplicates()
            df_dict[node] = df_dict[node].dropna(subset = [config['NODE_ID_FIELD'][node]])
            parent_id_validation_result_list = []
            missing_parent_id = False
            for parent_column in config['PARENT_MAPPING_COLUMNS']:
                if parent_column['node'] == node:
                    parent_id_field = parent_column['parent_node'] + '.' + parent_column['property']
                    relationship = parent_column["relationship"]
                    if model["Relationships"][relationship]["Mul"] == "many_to_many":
                        mul = "many_to_many"
                    if parent_id_field in df_dict[node].keys():
                        parent_id_validation_result_df = df_dict[node][df_dict[node][parent_id_field].isna()]
                        if len(parent_id_validation_result_df) > 0:
                            #df_dict[node] = df_dict[node].drop(parent_id_validation_result_df.index)
                            #parent_id_validation_result = list(set(list(parent_id_validation_result_df[config['NODE_ID_FIELD'][node]])))
                            parent_id_validation_result_list.append(list(set(list(parent_id_validation_result_df[config['NODE_ID_FIELD'][node]]))))
                            missing_parent_id = True
                        else:
                            parent_id_validation_result_list.append([])
            if missing_parent_id:
                parent_id_validation_result = list(set(parent_id_validation_result_list[0]).intersection(*parent_id_validation_result_list[1:]))
                if len(parent_id_validation_result) > 0:
                    cds_log.warning("The ID {}'s all parent_ids are NULL in the node {} from the study {}".format(parent_id_validation_result, node, raw_data_name))
                    df_dict[node] = df_dict[node].drop(df_dict[node][df_dict[node][config['NODE_ID_FIELD'][node]].isin(parent_id_validation_result)].index)
                    df_dict = delete_children(parent_mapping_column_list, parent_id_validation_result, node, df_dict, config)
                    for deleted_parent_id in parent_id_validation_result:
                        parent_id_validation_df_row = pd.DataFrame(data = [[node, deleted_parent_id, parent_id_field]], columns = ['node name', 'ID', 'parent ID field'])
                        parent_id_validation_df = pd.concat([parent_id_validation_df, parent_id_validation_df_row], ignore_index=True)
                    print_id_validation_result(parent_id_validation_df, config, cds_log, prefix, True)
            if node in config['NODE_ID_FIELD'].keys():
                #id_validation_result = [x for x in set(list(df_dict[node][config['NODE_ID_FIELD'][node]])) if list(df_dict[node][config['NODE_ID_FIELD'][node]]).count(x) > 1 or pd.isna(x) or "nan" in x]
                id_validation_result = [x for x in set(list(df_dict[node][config['NODE_ID_FIELD'][node]])) if list(df_dict[node][config['NODE_ID_FIELD'][node]]).count(x) > 1 or pd.isna(x)]
                new_id_validation_result = []
                many_to_many_id_list = []
                if len(id_validation_result) > 0:
                    deleted_record = df_dict[node][df_dict[node][config['NODE_ID_FIELD'][node]].isin(id_validation_result)]
                    for id in id_validation_result:
                        conflicted_column_names = []
                        for column_name in deleted_record.keys():
                            conflicted_column = False
                            deleted_id_df = deleted_record.loc[deleted_record[config['NODE_ID_FIELD'][node]] == id]
                            if len(set(list(deleted_id_df[column_name]))) > 1:
                                conflicted_column = True
                            if conflicted_column and column_name != config['NODE_ID_FIELD'][node]:
                                conflicted_column_names.append(column_name)

                        if mul == "many_to_many" and len(conflicted_column_names) == 1 and conflicted_column_names[0] not in model["Nodes"][node]["Props"]: #if the relationship is many to many and the only comflicted column is parent column, then we do nothing
                            many_to_many_id_list.append(id)
                        else:
                            id_validation_df_row = pd.DataFrame(data = [[node, id, conflicted_column_names]], columns = ['node name', 'ID', 'conflict property'])
                            id_validation_df = pd.concat([id_validation_df, id_validation_df_row], ignore_index=True)
                            new_id_validation_result.append(id)
                    if len(new_id_validation_result) > 0:
                        df_dict[node] = df_dict[node][~df_dict[node][config['NODE_ID_FIELD'][node]].isin(new_id_validation_result)]
                        df_dict = delete_children(parent_mapping_column_list, new_id_validation_result, node, df_dict, config)
                        cds_log.warning('The ID {} is duplicate in the node {} from the study {}'.format(new_id_validation_result, node, raw_data_name))
                        cds_log.warning('Removed all data related to the duplicated ID {} from the node {} from the study {}'.format(new_id_validation_result, node, raw_data_name))
                    if len(many_to_many_id_list) >0:
                        cds_log.warning('The ID {} is duplicate in the node {} from the study {} but match the many to many relationship'.format(many_to_many_id_list, node, raw_data_name))

    if len(id_validation_df) > 0:
        #prefix = df_dict['study']['phs_accession'][0]
        print_id_validation_result(id_validation_df, config, cds_log, prefix, False)
    return df_dict


def ssn_validation(df_dict, data_file, cds_log, file_validation_df):
    raw_data_name = os.path.basename(data_file)
    pattern_list = [r"\d{3}-\d{2}-\d{4}", r"\d{3}_\d{2}_\d{4}", r"(?<=\D)\d{9}(?=\D)"]
    df_nulllist = list(df_dict['file'].isnull().all(axis=1))
    if False in df_nulllist:
        cds_log.info('Start validating file name for {}'.format(raw_data_name))
        for file_name in df_dict['file']['file_name']:
            for pattern in pattern_list:
                matches = re.findall(pattern, file_name)
                if len(matches) > 0:
                    file_validation_df_new_row = pd.DataFrame()
                    file_validation_df_new_row['Raw_Data_File'] = [raw_data_name ]
                    file_validation_df_new_row['File_Name'] = [file_name]
                    file_validation_df_new_row['Suspicious_SSN'] = [str(matches)]
                    file_validation_df = pd.concat([file_validation_df, file_validation_df_new_row], ignore_index=True)
    return file_validation_df



def ui_validation(df_dict, config, data_file, cds_log, property_validation_df, model, data_file_base):
    # The function to do check if the UI related properties are in the transformed data files
    # "data_file" is the path of the raw data files
    # "config" is the config file
    # "cds_log" is the log object
    raw_data_name = os.path.basename(data_file)
    validation_df = pd.read_excel(io = config['VALIDATION_FILE'],
                                sheet_name =  "Mapping",
                                engine = "openpyxl",
                                keep_default_na = True)
    for node in df_dict.keys():
        df_nulllist = list(df_dict[node].isnull().all(axis=1))
        if False in df_nulllist:
            ui_properties = list(validation_df.loc[validation_df['Node Name'] == node, 'Property Name'])
            ui_properties = list(set([x for x in ui_properties if x != '-' and not pd.isna(x)]))
            #properties = model['Nodes'][node]['Props']
            if len(ui_properties) > 0:
                #for prop in properties:
                for prop in ui_properties:
                    if prop not in df_dict[node].keys() and prop in ui_properties:
                        if prop != "experimental_strategy_and_data_subtypes":
                            df_dict[node][prop] = ['Not specified in data'] * len(df_dict[node])
                        property_validation_df_new_row = pd.DataFrame()
                        property_validation_df_new_row['Missing_Properties'] = [node + '.' +prop]
                        property_validation_df_new_row['UI_Related'] = [True]
                        property_validation_df_new_row['Raw_Data_File'] = [data_file_base]
                        property_validation_df = pd.concat([property_validation_df, property_validation_df_new_row], ignore_index=True)

                        cds_log.warning('The data node {} does not have require UI property {} extracted from raw data file {}'.format(node, prop, raw_data_name))
                    elif prop in df_dict[node].keys() and prop in ui_properties and df_dict[node][prop].isnull().values.any():
                        if prop != "experimental_strategy_and_data_subtypes":
                            df_dict[node][prop] = df_dict[node][prop].replace(np.nan, 'Not specified in data')
                            #df_dict[node][prop] = df_dict[node][prop].replace('', 'Not specified in data')
                    '''
                    elif prop not in df_dict[node].keys() and prop not in ui_properties:
                        property_validation_df_new_row = pd.DataFrame()
                        property_validation_df_new_row['Missing_Properties'] = [node + '.' +prop]
                        property_validation_df_new_row['UI_Related'] = [False]
                        property_validation_df_new_row['Raw_Data_File'] = [data_file_base]
                        property_validation_df = pd.concat([property_validation_df, property_validation_df_new_row], ignore_index=True)
                    '''
    return df_dict, property_validation_df

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
            if key['Key'].endswith(".xlsx"):
                s3.download_file(config['S3_BUCKET'], key['Key'], file_key)

def combine_columns(df_dict, config, cds_log):
    for combine_node in config['COMBINE_COLUMN']:
        if combine_node['node'] in df_dict.keys():
            if combine_node['external_node'] == False:
                if combine_node['column1'] in df_dict[combine_node['node']].keys() and combine_node['column2'] in df_dict[combine_node['node']].keys():
                #df_dict[combine_node['node']][combine_node['new_column']] = df_dict[combine_node['node']][combine_node['column1']].astype(str) + "_" + df_dict[combine_node['node']][combine_node['column2']].astype(str)
                    if combine_node['new_column'] not in df_dict[combine_node['node']].keys():
                        df_dict[combine_node['node']][combine_node['new_column']] = [np.nan] * len(df_dict[combine_node['node']])
                    for i in range(0, len(df_dict[combine_node['node']])):
                        if not pd.isna(df_dict[combine_node['node']].loc[i, combine_node['column1']]) and not pd.isna(df_dict[combine_node['node']].loc[i, combine_node['column2']]):
                            string_value_1 = convert_to_string(df_dict[combine_node['node']].loc[i, combine_node['column1']])
                            string_value_2 = convert_to_string(df_dict[combine_node['node']].loc[i, combine_node['column2']])
                            df_dict[combine_node['node']].loc[i, combine_node['new_column']] = string_value_1 + "_" + string_value_2
                elif combine_node['column1'] not in df_dict[combine_node['node']].keys():
                    cds_log.info(f"{combine_node['column1']} not in {combine_node['node']}")
                else:
                    cds_log.info(f"{combine_node['column2']} not in {combine_node['node']}")
            else:
                #df_dict[combine_node['node']][combine_node['new_column']] = df_dict[combine_node['external_node']][combine_node['column1']].astype(str) + "_" + df_dict[combine_node['node']][combine_node['column2']].astype(str)
                if combine_node['column1'] in df_dict[combine_node['external_node']].keys() and combine_node['column2'] in df_dict[combine_node['node']].keys():
                    if combine_node['new_column'] not in df_dict[combine_node['node']].keys():
                        df_dict[combine_node['node']][combine_node['new_column']] = [np.nan] * len(df_dict[combine_node['node']])
                    for i in range(0, len(df_dict[combine_node['node']])):
                        if not pd.isna(df_dict[combine_node['external_node']].loc[i, combine_node['column1']]) and not pd.isna(df_dict[combine_node['node']].loc[i, combine_node['column2']]):
                            string_value_1 = convert_to_string(df_dict[combine_node['external_node']].loc[i, combine_node['column1']])                         
                            string_value_2 = convert_to_string(df_dict[combine_node['node']].loc[i, combine_node['column2']])
                            df_dict[combine_node['node']].loc[i, combine_node['new_column']] = string_value_1 + "_" + string_value_2
                elif combine_node['column1'] not in df_dict[combine_node['external_node']].keys():
                    cds_log.info(f"{combine_node['column1']} not in {combine_node['external_node']}")
                else:
                    cds_log.info(f"{combine_node['column2']} not in {combine_node['node']}")
    return df_dict

def convert_to_string(value):
    if isinstance(value, float):
        if value.is_integer():
            int_value = int(value)
            return str(int_value)
    return str(value)    

def add_secondary_id(df_dict, config, cds_log):
    try:
        for secondary_id_node in config['SECONDARY_ID_COLUMN']:
            if secondary_id_node['node'] in df_dict.keys():
                df_nulllist = list(df_dict[secondary_id_node['node']].isnull().all(axis=1))
                if False in df_nulllist:
                    if secondary_id_node['node_id'] not in df_dict[secondary_id_node['node']].keys():
                        cds_log.warning('The ID {} is missing and will be replaced by {} for the node {}'.format(secondary_id_node['node_id'], secondary_id_node['secondary_id'], secondary_id_node['node']))
                        parent_node = secondary_id_node['secondary_id'].split('.')[0]
                        parent_node_id = secondary_id_node['secondary_id'].split('.')[1]
                        for i in range(0, len(df_dict[secondary_id_node['node']])):
                            df_dict[secondary_id_node['node']].loc[i, secondary_id_node['node_id']] = df_dict[parent_node].loc[i, parent_node_id]
                    #df_dict[secondary_id_node['node']][secondary_id_node['node_id']] = df_dict[parent_node][parent_node_id]
    except Exception as e:
        cds_log.info("Unable to create secondary ID")
        cds_log.error(e)
    return df_dict

def add_historical_value(df_dict, config, cds_log):
    if 'HISTORICAL_PROPERTIES' in config.keys():
        for historical_property in config['HISTORICAL_PROPERTIES']:
            historical_property_value = list(df_dict[historical_property['node']][historical_property['property']])[0]
            if historical_property_value is None:
                cds_log.error(f"{historical_property['property']} is None, abort transformation")
                sys.exit(1)
            historical_property_key = list(df_dict[historical_property['node']][config['NODE_ID_FIELD'][historical_property['node']]])[0]
            historical_property_value_list = historical_property_value.split(",")
            historical_property_value_list = [i.strip() for i in historical_property_value_list]
            historical_property_file = historical_property['historical_property_file']
            with open(historical_property_file) as f:
                history_value = yaml.safe_load(f)
            if history_value is None:
                history_value = {}
                history_value[historical_property_key] = historical_property_value_list
            else:
                if historical_property_key not in history_value.keys():
                    history_value[historical_property_key] = historical_property_value_list
                else:
                    new_historical_value_list = list(set(historical_property_value_list) - set(history_value[historical_property_key]))
                    if len(new_historical_value_list) > 0:
                        history_value[historical_property_key] += new_historical_value_list
            history_value[historical_property_key].sort(reverse=True)
            with open(historical_property_file, 'w') as yaml_file:
                yaml.dump(history_value, yaml_file, default_flow_style=False, width=10000)

#Update the study version for all study files
def print_historical_value(config, cds_log):
    output_folder = os.path.join(config['OUTPUT_FOLDER'], config['DATA_BATCH_NAME'])
    for data_file in glob.glob('{}/*.tsv'.format(output_folder)):
        for historical_property in config['HISTORICAL_PROPERTIES']:
            with open(historical_property['historical_property_file']) as f:
                history_value = yaml.safe_load(f)
            suffix = "-" + historical_property['node']
            if suffix in os.path.splitext(os.path.basename(data_file))[0]:
                cds_log.info(f"Start updating the historical property {historical_property['property']} for transformed file {os.path.basename(data_file)}")
                history_df = pd.read_csv(data_file, sep='\t')
                new_history_value = ""
                historical_property_key = list(history_df[config['NODE_ID_FIELD'][historical_property['node']]])[0]
                for i in range(0, len(history_value[historical_property_key])):
                    if i == 0:
                        new_history_value = history_value[historical_property_key][i]
                    else:
                        new_history_value = new_history_value + "," + history_value[historical_property_key][i]
                history_df[historical_property["property"]] = [new_history_value] * len(history_df)
                history_df.to_csv(data_file, sep = "\t", index = False)