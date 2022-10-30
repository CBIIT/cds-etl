import pandas as pd
import os
import yaml
import argparse
from difflib import SequenceMatcher
import numpy as np
import glob


parser = argparse.ArgumentParser()
parser.add_argument('config_file')
args = parser.parse_args()
config = args.config_file

def match_col(cds_df, property):
    limit = 0.75
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


def extract_data(cds_df, model, node):
    parent_mapping_column_list = [
        {'node': 'participant', 'parent_node': 'study', 'property': 'phs_accession'},
        {'node': 'sample', 'parent_node': 'participant', 'property': 'participant_id'},
        {'node': 'file', 'parent_node': 'sample', 'property': 'sample_id'}
    ]
    new_df = pd.DataFrame()
    for property in model['Nodes'][node]['Props']:
        col = match_col(cds_df, property)
        if col != None:
            if not cds_df[col].isnull().all():
                new_df[property] = cds_df[col]
            #print(property, col, node)
    new_df_nulllist = list(new_df.isnull().all(axis=1))
    if False in new_df_nulllist:
        new_df['type'] = [node] * len(new_df)
        for parent_mapping_column in parent_mapping_column_list:
            if node == parent_mapping_column['node']:
                if parent_mapping_column['property'] in cds_df.keys():
                    new_df[parent_mapping_column['property']] = cds_df[parent_mapping_column['property']]
    #new_df = new_df.drop_duplicates()
    return new_df

def remove_node(dict):
    nodes = [
        'diagnosis',
        'treatment'
    ]
    for node in nodes:
        dict.pop(node)
    return dict

#Print data
def print_data(df, config, file_name, data_file):
    sub_folder_name_list = os.path.basename(data_file).split('.')
    sub_folder_name = sub_folder_name_list[0]
    sub_folder = os.path.join(config['OUTPUT_FOLDER'], sub_folder_name)
    file_name = file_name + '.tsv'
    file_name = os.path.join(sub_folder, file_name)
    if not os.path.exists(sub_folder):
       os.makedirs(sub_folder)
    df_nulllist = list(df.isnull().all(axis=1))
    if False in df_nulllist:
        df.to_csv(file_name, sep = "\t", index = False)
        print(f'Data node {node} is created and stored in {sub_folder}')


with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
path = os.path.join(config['DATA_FOLDER'], '*.xlsx')
for data_file in glob.glob(path):
    data_file_base = os.path.basename(data_file)
    data_file_sheet_name = os.path.splitext(data_file_base)[0]
    print(f'Start transforming {data_file_base}')

    df_dict = {}
    Participant = pd.read_excel(io = data_file,
                            sheet_name =  data_file_sheet_name,
                            engine = "openpyxl",
                            keep_default_na = False)
    Participant = Participant.replace(r'^\s*$', np.nan, regex=True)

    with open(config['NODE_FILE']) as f:
        model = yaml.load(f, Loader = yaml.FullLoader)
    for node in model['Nodes']:
        df_dict[node] = extract_data(Participant, model, node)
        df_dict[node] = df_dict[node].drop_duplicates()
    print(df_dict.keys())
    df_dict = remove_node(df_dict)
    for node in df_dict.keys():
        print_data(df_dict[node], config, node, data_file)
