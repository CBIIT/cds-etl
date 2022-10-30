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

def match_property(model, node, col):
    limit = 0.75
    col_name = col.replace(" ", "_").lower()
    property_list = []
    for property in model['Nodes'][node]['Props']:
        s = SequenceMatcher(None)
        s.set_seq1(col_name)
        s.set_seq2(property)
        ratio = s.ratio()
        if ratio >= limit:
            property_list.append({'ratio': ratio, 'property': property})
    if len(property_list) == 0:
        return None
    max_index =  max(range(len(property_list)), key=lambda index: property_list[index]['ratio'])
    similar_property = property_list[max_index]['property']
    return similar_property


def extract_data(cds_df, model, node):
    new_df = pd.DataFrame()
    for col in cds_df.columns:
        property = match_property(model, node, col)
        if property != None:
            if not cds_df[col].isnull().all():
                new_df[property] = cds_df[col]
            #print(property, col, node)
    new_df_nulllist = list(new_df.isnull().all(axis=1))
    if False in new_df_nulllist:
        new_df['type'] = [node] * len(new_df)
    #new_df = new_df.drop_duplicates()
    return new_df



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
    print(f'Start transforming {os.path.basename(data_file)}')
    Participant = pd.read_excel(io = data_file,
                            sheet_name = "Participant",
                            engine = "openpyxl",
                            keep_default_na = False)
    Sample = pd.read_excel(io = data_file,
                            sheet_name = "Sample",
                            engine = "openpyxl",
                            keep_default_na = False)
    File = pd.read_excel(io = data_file,
                            sheet_name = "File",
                            engine = "openpyxl",
                            keep_default_na = False)
    Genomic_Info = pd.read_excel(io = data_file,
                            sheet_name = "Genomic Info",
                            engine = "openpyxl",
                            keep_default_na = False)

    Study = pd.read_excel(io = data_file,
                            sheet_name = "Study",
                            engine = "openpyxl",
                            keep_default_na = False)

    File_Participant_Sample = pd.read_excel(io = data_file,
                            sheet_name = "File-Participant-Sample Mapping",
                            engine = "openpyxl",
                            keep_default_na = False)

    Participant = Participant.replace(r'^\s*$', np.nan, regex=True)
    Sample = Sample.replace(r'^\s*$', np.nan, regex=True)
    File = File.replace(r'^\s*$', np.nan, regex=True)
    Genomic_Info = Genomic_Info.replace(r'^\s*$', np.nan, regex=True)
    Study = Study.replace(r'^\s*$', np.nan, regex=True)
    File_Participant_Sample = File_Participant_Sample.replace(r'^\s*$', np.nan, regex=True)


    with open(config['NODE_FILE']) as f:
        model = yaml.load(f, Loader = yaml.FullLoader)
    df_dict = {}


    file_df = extract_data(File, model, 'file')
    genomic_info_df = extract_data(Genomic_Info, model, 'genomic_info')
    participant_df = extract_data(Participant, model, 'participant')
    study_df = extract_data(Study, model, 'study')
    sample_df = extract_data(Sample, model, 'sample')
    df_dict['file'] = file_df
    df_dict['participant'] = participant_df
    df_dict['study'] = study_df
    df_dict['sample'] = sample_df
    df_dict['genomic_info'] = genomic_info_df

    participant_nulllist = list(df_dict['participant'].isnull().all(axis=1))
    if len(df_dict['study'] == 1) and 'phs_accession' in df_dict['study'].keys() and False in participant_nulllist:
        if not df_dict['study']['phs_accession'].isnull().values.any():
            df_dict['participant']['study.phs_accession'] = list(df_dict['study']['phs_accession']) * len(df_dict['participant'])

    if not File_Participant_Sample['sample_id'].isnull().values.any() and not File_Participant_Sample['participant_id'].isnull().values.any():
        participant_id_list = []
        for sample_id in df_dict['sample']['sample_id']:
            try:
                participant_id_list.append(File_Participant_Sample.loc[File_Participant_Sample['sample_id'] == sample_id, 'participant_id'].iloc[0])
            except:
                participant_id_list.append(None)
        if None not in participant_id_list:
            df_dict['sample']['participant.participant_id'] = participant_id_list

    if not File_Participant_Sample['file_id'].isnull().values.any() and not File_Participant_Sample['sample_id'].isnull().values.any():
        sample_id_list = []
        for file_id in df_dict['file']['file_id']:
            try:
                sample_id_list.append(File_Participant_Sample.loc[File_Participant_Sample['file_id'] == file_id, 'sample_id'].iloc[0])
            except:
                sample_id_list.append(None)
        if None not in participant_id_list:
            df_dict['file']['sample.sample_id'] = sample_id_list



    for node in df_dict.keys():
        print_data(df_dict[node], config, node, data_file)
