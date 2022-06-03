import pandas as pd
import os
import yaml
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('config_file')
args = parser.parse_args()
config = args.config_file

def extract_data(df, model, df_list, node):
    for cds_df in df_list:
        for property in model['Nodes'][node]['Props']:
            for col in cds_df.columns:
                col_name = col.replace(" ", "_").lower()
                if col_name in property or property in col_name:
                    #if len(cds_df) > len(df):
                    df[property] = cds_df[col]
            if property == 'file_name' and 'filename' in df.keys():
                df['file_name'] = cds_df['filename']
            if property == 'file_type' and 'filetype' in df.keys():
                df['file_type'] = cds_df['filetype']
            df['type'] = [node] * len(df)
    return df


#Add acl to file node
def add_properties(file_name, df, cds_df):
    props = [
        {'node':'file', 'new_property':'acl', 'new_value': cds_df['acl']},
        {'node':'file', 'new_property':'file_id', 'new_value': cds_df['GUID']},
        {'node':'file', 'new_property':'file_description', 'new_value': cds_df['title']},
        {'node':'genomic_info', 'new_property':'file.file_id', 'new_value': cds_df['GUID']},
        {'node':'file', 'new_property':'sample.sample_id', 'new_value': cds_df['sample_id']},
        {'node':'genomic_info', 'new_property':'platform', 'new_value': cds_df['library_platform']},
    ]
    for property in props:
        if property['node'] == file_name:
             df[property['new_property']] = property['new_value']
    
    return df
#Remove node
def remove_node(df, file_name):
    nodes = [
        {'node':'diagnosis'},
        {'node':'participant'}
    ]
    for node in nodes:
        if node['node'] == file_name:
            df = pd.DataFrame()
    return df
#Print data
def print_data(df, config, file_name):
    file_name = config['OUTPUT_FOLDER'] + file_name + '.tsv'
    if not os.path.exists(config['OUTPUT_FOLDER']):
        os.mkdir(config['OUTPUT_FOLDER'])
    df.to_csv(file_name, sep = "\t", index = False)


with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)

CDS_Manifest_df = pd.read_excel(io = config['DATA_FILE1'],
                        sheet_name = "CDS Manifest",
                        engine = "openpyxl",
                        keep_default_na = False)
CGC_CDS_Explorer_df = pd.read_excel(io = config['DATA_FILE1'],
                        sheet_name = "CGC CDS Explorer",
                        engine = "openpyxl",
                        keep_default_na = False)
SRA_Run_Selector_df = pd.read_excel(io = config['DATA_FILE1'],
                        sheet_name = "SRA Run Selector",
                        engine = "openpyxl",
                        keep_default_na = False)
Participant = pd.read_excel(io = config['DATA_FILE2'],
                        sheet_name = "Participant",
                        engine = "openpyxl",
                        keep_default_na = False)
Sample = pd.read_excel(io = config['DATA_FILE2'],
                        sheet_name = "Sample",
                        engine = "openpyxl",
                        keep_default_na = False)
File = pd.read_excel(io = config['DATA_FILE2'],
                        sheet_name = "File",
                        engine = "openpyxl",
                        keep_default_na = False)
Genomic_Info = pd.read_excel(io = config['DATA_FILE2'],
                        sheet_name = "Genomic Info",
                        engine = "openpyxl",
                        keep_default_na = False)
Study = pd.read_excel(io = config['DATA_FILE2'],
                        sheet_name = "Study",
                        header = None,
                        engine = "openpyxl",
                        keep_default_na = False)

header_list = Study.values.T[0].tolist()
value_list = Study.values.T[1].tolist()

Study_df = pd.DataFrame()
for index in range(0, len(header_list)):
    if value_list[index] != '':
        Study_df[header_list[index]] = [value_list[index]] * len(CDS_Manifest_df)

with open(config['NODE_FILE']) as f:
    model = yaml.load(f, Loader = yaml.FullLoader)
df_dict = {}
for node in model['Nodes']:
    #print(node)
    df = pd.DataFrame()
    df_list = [Sample, File, Genomic_Info, CDS_Manifest_df, CGC_CDS_Explorer_df, SRA_Run_Selector_df, Study_df]
    df = extract_data(df, model, df_list, node)
    df = add_properties(node, df, CDS_Manifest_df)
    df = remove_node(df, node)
    #participant should only come from Participant sheet
    df = extract_data(df, model, [Participant], node)
    if len(df) > 0:
        df_dict[node] = df

#improve the data after extraction
df_dict['sample']['participant.participant_id '] = CGC_CDS_Explorer_df['Participant ID']
df_dict['participant']['study.phs_accession'] = df_dict['study']['phs_accession'][0:len(df_dict['participant'])]
df_dict['study'] = df_dict['study'].drop(columns = ['size_of_data_being_uploaded', 'study_external_url'])
df_dict['study'] =  df_dict['study'].drop_duplicates()
for node in df_dict.keys():
    print_data(df_dict[node], config, node)
    print(f'Data node {node} is created')
