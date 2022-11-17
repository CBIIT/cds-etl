import pandas as pd
import os
import yaml
import argparse
from difflib import SequenceMatcher
import numpy as np
import glob
import dateutil.tz
import datetime
from cds_transformation_functions import clean_data, print_data, upload_files
import random
from bento.common.utils import get_logger

cds_log = get_logger('CDS V1.2 Transformation Script')

def match_property(model, node, col, limit):
    # The function to match the column name from the raw files and the properties from the model file
    # The function will create a list of candidate properties for each column name base on the similarity
    # The "limit" will decide the minimum similarity for the property to be added into the candidate list
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

def extract_raw_data_dict(cds_df, model, node, limit, raw_dict):
    # The function to extract raw data dictionary from the raw data files
    # "cds_df" is the raw data data frame
    # "model" is the data model from the model file
    # "node" is the node name of the current node for extracting
    # "limit" is the minimum similarity
    # "raw_dict" is the raw data dictionary
    for col in cds_df.columns:
        property = match_property(model, node, col, limit)
        if property != None:
            if not cds_df[col].isnull().all():
                #new_df[property] = cds_df[col]
                if node not in raw_dict.keys():
                    raw_dict[node] = {}
                raw_dict[node][col] = property
    return raw_dict

def extract_data(cds_df, node, raw_data_dict):
    # The function to extract data from the raw data files
    # "cds_df" is the raw data data frame
    # "raw_data_dict" is the raw data dictionary, this function use the raw data dictionary to locate the corresponding
    with open(raw_data_dict) as f:
        raw_dict = yaml.load(f, Loader = yaml.FullLoader)
    new_df = pd.DataFrame()
    for col in cds_df.columns:
        if node in raw_dict.keys():
            if col in raw_dict[node].keys() and not cds_df[col].isnull().all():
                new_df[raw_dict[node][col]] = cds_df[col]
            #print(property, col, node)
    new_df_nulllist = list(new_df.isnull().all(axis=1))
    if False in new_df_nulllist:
        if node == 'file' and 'file_id' not in new_df.keys():
            if 'GUID' in cds_df.keys():
                new_df['file_id'] = cds_df['GUID']
            elif 'guid' in cds_df.keys():
                new_df['file_id'] = cds_df['guid']
            else:
                file_id = random.sample(range(10**9, 10**10), len(new_df))
                new_df['file_id'] = file_id
        # If the extracted dataframe not only consist with NAN values, then add the 'type' property to the dataframe
        new_df['type'] = [node] * len(new_df)
    #new_df = new_df.drop_duplicates()
    return new_df

parser = argparse.ArgumentParser()
parser.add_argument('--config_file', type=str, help='The path of the config file.', required=True) #Argument about the config file
parser.add_argument('--upload_s3', help='Decide whether or not upload the transformed data to s3', action='store_true')
parser.add_argument('--extract_raw_data_dictionary', help='Decide whether or not extract raw data dictionary instead of transformed raw data', action='store_true')
args = parser.parse_args()
config = args.config_file

with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
path = os.path.join(config['DATA_FOLDER'], config['DATA_BATCH_NAME'], '*.xlsx')
ratio_limit = config['RATIO_LIMIT']
eastern = dateutil.tz.gettz('US/Eastern')
timestamp = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%dT%H%M%S")
if args.extract_raw_data_dictionary == False:
    for data_file in glob.glob(path):
        # The for loop will grab all the EXCEL files from the raw data folder
        cds_log.info(f'Start transforming {os.path.basename(data_file)}')
        # 'io' is the path of the excel file
        # 'sheet_name' is the sheet's name of the table we are going to read in
        # 'engine' is the engine used for reading in the data from excel
        # 'openpyxl' needs to install first and it is in the requirement.txt
        # 'keep_default_na' is whether or not to include the default NaN values when parsing the data.
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
        Diagnosis = pd.read_excel(io = data_file,
                                sheet_name = "Diagnosis (opt)",
                                engine = "openpyxl",
                                keep_default_na = False)

        # Replace all the empty string with NAN values
        Participant = Participant.replace(r'^\s*$', np.nan, regex=True)
        Sample = Sample.replace(r'^\s*$', np.nan, regex=True)
        File = File.replace(r'^\s*$', np.nan, regex=True)
        Genomic_Info = Genomic_Info.replace(r'^\s*$', np.nan, regex=True)
        Study = Study.replace(r'^\s*$', np.nan, regex=True)
        File_Participant_Sample = File_Participant_Sample.replace(r'^\s*$', np.nan, regex=True)
        Diagnosis = Diagnosis.replace(r'^\s*$', np.nan, regex=True)

        with open(config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        df_dict = {}
        raw_data_dict = config['RAW_DATA_DICTIONARY']
        # Extract dataframe based on different sheet
        file_df = extract_data(File, 'file', raw_data_dict)
        genomic_info_df = extract_data(Genomic_Info, 'genomic_info', raw_data_dict)
        participant_df = extract_data(Participant, 'participant', raw_data_dict)
        study_df = extract_data(Study, 'study', raw_data_dict)
        sample_df = extract_data(Sample, 'sample', raw_data_dict)
        diagnosis_df = extract_data(Diagnosis, 'diagnosis', raw_data_dict)
        df_dict['file'] = file_df
        df_dict['participant'] = participant_df
        df_dict['study'] = study_df
        df_dict['sample'] = sample_df
        df_dict['genomic_info'] = genomic_info_df
        df_dict['diagnosis'] = diagnosis_df
        if 'participant_id' in Diagnosis.keys():
            if not Diagnosis['participant_id'].isnull().all():
                df_dict['diagnosis']['participant.participant_id'] = Diagnosis['participant_id']
        participant_nulllist = list(df_dict['participant'].isnull().all(axis=1))
        if len(df_dict['study'] == 1) and 'phs_accession' in df_dict['study'].keys() and False in participant_nulllist:
            # If the participant data frame is not null and the study dataframe has only one record and 'phs_accession' is in the study's dataframe
            # Then the participant data frame can has it's parent mapping column
            if not df_dict['study']['phs_accession'].isnull().values.any():
                df_dict['participant']['study.phs_accession'] = list(df_dict['study']['phs_accession']) * len(df_dict['participant'])

        if not File_Participant_Sample['sample_id'].isnull().values.any() and not File_Participant_Sample['participant_id'].isnull().values.any():
            # If the 'sample_id' column from the sheet File_Participant_Sample is not completely empty
            # And if the 'participant_id' from the sheet File_Participant_Sample is not completely empty
            # Then start build a parent mapping column for the sample based on 'sample_id'
            participant_id_list = []
            for sample_id in df_dict['sample']['sample_id']:
                try:
                    participant_id_list.append(File_Participant_Sample.loc[File_Participant_Sample['sample_id'] == sample_id, 'participant_id'].iloc[0])
                except:
                    participant_id_list.append(None)
            if None not in participant_id_list:
                # Parent mapping column can not have none values
                df_dict['sample']['participant.participant_id'] = participant_id_list

        if not File_Participant_Sample['file_id'].isnull().values.any() and not File_Participant_Sample['sample_id'].isnull().values.any():
            # If the 'file_id' column from the sheet File_Participant_Sample is not completely empty
            # And if the 'sample_id' from the sheet File_Participant_Sample is not completely empty
            # Then start build a parent mapping column for the file based on 'file_id'
            sample_id_list = []
            for file_id in df_dict['file']['file_id']:
                try:
                    sample_id_list.append(File_Participant_Sample.loc[File_Participant_Sample['file_id'] == file_id, 'sample_id'].iloc[0])
                except:
                    sample_id_list.append(None)
            if None not in participant_id_list:
                # Parent mapping column can not have none values
                df_dict['file']['sample.sample_id'] = sample_id_list

        df_dict = clean_data(df_dict, config)
        prefix = df_dict['study']['phs_accession'][0]
        print_data(df_dict, config, cds_log, prefix)
        if args.upload_s3 == True:
            upload_files(config, timestamp, cds_log)
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
        Diagnosis = pd.read_excel(io = data_file,
                                sheet_name = "Diagnosis (opt)",
                                engine = "openpyxl",
                                keep_default_na = False)
        # Replace all the empty string with NAN values
        Participant = Participant.replace(r'^\s*$', np.nan, regex=True)
        Sample = Sample.replace(r'^\s*$', np.nan, regex=True)
        File = File.replace(r'^\s*$', np.nan, regex=True)
        Genomic_Info = Genomic_Info.replace(r'^\s*$', np.nan, regex=True)
        Study = Study.replace(r'^\s*$', np.nan, regex=True)
        Diagnosis = Diagnosis.replace(r'^\s*$', np.nan, regex=True)
        with open(config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        raw_dict = extract_raw_data_dict(File, model, 'file', ratio_limit, raw_dict)
        raw_dict = extract_raw_data_dict(Genomic_Info, model, 'genomic_info', ratio_limit, raw_dict)
        raw_dict = extract_raw_data_dict(Participant, model, 'participant', ratio_limit, raw_dict)
        raw_dict = extract_raw_data_dict(Study, model, 'study',  ratio_limit, raw_dict)
        raw_dict = extract_raw_data_dict(Sample, model, 'sample',  ratio_limit, raw_dict)
        raw_dict = extract_raw_data_dict(Diagnosis, model, 'diagnosis',  ratio_limit, raw_dict)

    with open(config['RAW_DATA_DICTIONARY'], 'w') as outfile:
        yaml.dump(raw_dict, outfile, default_flow_style=False)
    cds_log.info('Raw data dictionary is stored in {}'.format(config['RAW_DATA_DICTIONARY']))
