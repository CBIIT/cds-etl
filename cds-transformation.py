import pandas as pd
import os
import yaml
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('config_file')
args = parser.parse_args()
config = args.config_file

def extract_data(df, model, cds_df):
    for property in model['Nodes'][node]['Props']:
        for col in cds_df.columns:
            if col in property:
                df[property] = cds_df[col]
        if property == 'file_name':
            df['file_name'] = cds_df['filename']
        if property == 'file_type':
            df['file_type'] = cds_df['filetype']
    return df
#Add acl to file node
def add_properties(file_name, df, cds_df):
    props = [
        {'node':'file', 'new_property':'acl', 'new_value': cds_df['acl']},
        {'node':'file', 'new_property':'file_id', 'new_value': cds_df['GUID']},
        {'node':'file', 'new_property':'file_description', 'new_value': cds_df['title']},
        {'node':'file', 'new_property':'genomic_info.library_id', 'new_value': cds_df['library_id']},
        {'node':'file', 'new_property':'sample.sample_id', 'new_value': cds_df['sample_id']},
        {'node':'genomic_info', 'new_property':'platform', 'new_value': cds_df['library_platform']}
    ]
    for property in props:
        if property['node'] == file_name:
             df[property['new_property']] = property['new_value']
    
    return df
#Remove node
def remove_node(df, file_name):
    nodes = [
        {'node':'study'},
        {'node':'sample'}
    ]
    for node in nodes:
        if node['node'] == file_name:
            df = pd.DataFrame()
    return df
#Print data
def print_data(config, file_name):
    file_name = config['OUTPUT_FOLDER'] + file_name + '.tsv'
    if not os.path.exists(config['OUTPUT_FOLDER']):
        os.mkdir(config['OUTPUT_FOLDER'])
    df.to_csv(file_name, sep = "\t", index = False)


with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
cds_df = pd.read_excel(io = config['DATA_FILE'],
                        sheet_name = "phs001437_cds_submission _resul",
                        engine = "openpyxl",
                        keep_default_na = False)
with open(config['NODE_FILE']) as f:
    model = yaml.load(f, Loader = yaml.FullLoader)    
for node in model['Nodes']:
    df = pd.DataFrame()
    df = extract_data(df, model, cds_df)
    df = add_properties(node, df, cds_df)
    df = remove_node(df, node)
    if len(df) > 0:
        print_data(config, node)
        print(f'Data node {node} is created')