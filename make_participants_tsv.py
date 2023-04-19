#!/usr/bin/env python3

import pandas as pd
import re

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# Data Sources

# Download Tabulated Datasets from NDA
#   Do not load this entire file into a pandas dataframe, it will take too much memory instead selectively load necessary columns
#   TODO: The ABCD4.0_MASTER_DATA_FILE is a compilation of data sources. Figure out the actual data sources within the Tabulated BDatasets and Raw Behavioral Data: 
#         https://nda.nih.gov/general-query.html?q=query=featured-datasets:Adolescent%20Brain%20Cognitive%20Development%20Study%20(ABCD)
tabulated_data_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD4.0_MASTER_DATA_FILE.csv'
# MRI Scanner info origin: https://nda.nih.gov/data_structure.html?short_name=abcd_mri01
mri_info_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_mri01_20230407/abcd_mri01.txt'
# QC info origin: https://nda.nih.gov/data_structure.html?short_name=abcd_fastqc01
fastqc01_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_fastqc01-20211221.txt'
# Collection 3165 datastructure manifest origin: https://nda.nih.gov/edit_collection.html?id=3165
c3165_manifest_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_collection-3165-20230407/datastructure_manifest.txt'
# Download previous participants_v1.0.0 from the collection to pull the matched groups. Original origin: https://github.com/DCAN-Labs/automated-subset-analysis
#   TODO: Determine more legitimate source for the matched group info (Box directory with the ABCD 2.0 Release)
matched_groups_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/participants_v1.0.0/participants.tsv'

# Hashmap of column name in Tabulated Datasets to that of the participants.tsv
tabulated_data_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "sex": "sex",
    "demo_race_a_p___10": "demo_race_a_p___10",
    "demo_race_a_p___11": "demo_race_a_p___11",
    "demo_race_a_p___12": "demo_race_a_p___12",
    "demo_race_a_p___13": "demo_race_a_p___13",
    "demo_race_a_p___14": "demo_race_a_p___14",
    "demo_race_a_p___15": "demo_race_a_p___15",
    "demo_race_a_p___16": "demo_race_a_p___16",
    "demo_race_a_p___17": "demo_race_a_p___17",
    "demo_race_a_p___18": "demo_race_a_p___18",
    "demo_race_a_p___19": "demo_race_a_p___19",
    "demo_race_a_p___20": "demo_race_a_p___20",
    "demo_race_a_p___21": "demo_race_a_p___21",
    "demo_race_a_p___22": "demo_race_a_p___22",
    "demo_race_a_p___23": "demo_race_a_p___23",
    "demo_race_a_p___24": "demo_race_a_p___24",
    "demo_race_a_p___25": "demo_race_a_p___25",
    "demo_race_a_p___77": "demo_race_a_p___77",
    "demo_race_a_p___99": "demo_race_a_p___99",
    "demo_ethn_p": "latinx",
    "age": "age",
    "ehi_handedness": "handedness",
    "devhx_5_twin_p": "siblings_twins",
    "demo_comb_income_p_l": "income",
    "demo_ed_p_l": "participant_education",
    "demo_prnt_ed_p_l": "parental_education",
    "medhx_9a_anesthesia_p_l": "anesthesia_exposure",
    "neurocog_pc1.bl": "pc1",
    "neurocog_pc2.bl": "pc2",
    "neurocog_pc3.bl": "pc3"
}

# Verify that all required columns exist
#tabulated_datasets_df = pd.read_csv('/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD4.0_MASTER_DATA_FILE.csv')
#for col in tabulated_data_map.keys():
#    print(f'{col} {col in tabulated_datasets_df.columns}')

# Columns populated by querying the sidecar json after dcm2bids OR from https://nda.nih.gov/data_structure.html?short_name=abcd_mri01
mri_info_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "mri_info_manufacturer": "scanner_manufacturer",
    "mri_info_manufacturersmn": "scanner_model",
    "mri_info_softwareversion": "scanner_software"
}

# DCAN specific variables
# TODO: Determine origin of matched groups or how to create it as more subjects are added
# Collection 3165 variable should be derived from subjects that exist on the collection 3165 datastructure manifest
dcan_data_list = [
    "collection_3165",
    "matched_groups"
]

# NeuroCog principal component scores: search for pc1, pc2, and pc3 in https://deap.nimhda.org/applications/Ontology/hierarchy.php?entry=display
pc_map = {
    "pc1": "neurocog_pc1.bl",
    "pc2": "neurocog_pc2.bl",
    "pc3": "eurocog_pc3.bl"
}

# Hashmap of session variable in the Tabulated Dataset to BIDS format for participants.tsv
bids_session_dict = {
    'baseline_year_1_arm_1': 'ses-baselineYear1Arm1',
    '6_month_follow_up_arm_1': 'ses-6MonthFollowUpArm1',
    '1_year_follow_up_y_arm_1': 'ses-1YearFollowUpYArm1',
    '18_month_follow_up_arm_1': 'ses-18MonthFollowUpArm1',
    '2_year_follow_up_y_arm_1':'ses-2YearFollowUpYArm1',
    '30_month_follow_up_arm_1': 'ses-30MonthFollowUpArm1',
    '3_year_follow_up_y_arm_1': 'ses-3YearFollowUpYArm1',
    '42_month_follow_up_arm_1': 'ses-42MonthFollowUpArm1'
}

sex_dict = {
    'M': 1,
    'F': 2,
}

# Select columns from the main Tabulated Datasets df
tabulated_data_df = pd.read_csv(tabulated_data_path, usecols=tabulated_data_map.keys())

# Select columns from the mri_info df
mri_info_df = pd.read_csv(mri_info_path, delimiter='\t', usecols=mri_info_map.keys())

# Merge the two dataframes
participants_df = pd.merge(tabulated_data_df, mri_info_df, how='right', on=['subjectkey', 'eventname'])

participants_df[["mri_info_manufacturer", "mri_info_manufacturersmn", "mri_info_softwareversion"]].drop_duplicates().sort_values(by=["mri_info_manufacturer", "mri_info_manufacturersmn", "mri_info_softwareversion"])

def format_participant_id_to_bids():
    #TODO
    return

def format_session_id_to_bids():
    #TODO
    return

#TODO: Remove lines without imaging data (use fastqc01.txt for this - maybe integrate with audit repo)
qc_df = pd.read_csv(fastqc01_path, delimiter='\t')

#TODO: Use Collection 3165 datastructure_manifest.txt to identify which subjects have been shared in order to populate Collection 3165 column

c3165_manifest_df = pd.read_csv(c3165_manifest_path, delimiter='\t')
# Get list of basenames + session
subjects = c3165_manifest_df['manifest_name'].apply(lambda x: re.match('sub-NDARINV[A-Z0-9]{8}', x).group(0) if re.search('sub-NDARINV[A-Z0-9]{8}', x) else None).unique()

no_sub_df = c3165_manifest_df['manifest_name'].apply(lambda x: re.sub('sub-NDARINV[A-Z0-9]{8}', '', x))
unique_basenames = no_sub_df.value_counts().sort_index(ascending=True)
print(unique_basenames.to_string())

#TODO: Matched Group
matched_groups_df = pd.read_csv(matched_groups_path, delimiter='\t', usecols=['participant_id', 'session_id', 'matched_group'])

