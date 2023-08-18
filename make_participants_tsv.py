#!/usr/bin/env python3

import pandas as pd
import re
#import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# participants.tsv creation process
#   1. Initialize participants.tsv subjects and sessions using the fastqc01 spreadsheet
#   2. Add demographic information from the NDA Dictionary resources and tabulated datasets
#   3. Add MRI information from the mri01 spreadsheet via the NDA Dictionary source
#   4. Add matched group information from the original participants.tsv

# Data Sources

# Download Tabulated Datasets from NDA
#   Do not load this entire file into a pandas dataframe, it will take too much memory instead selectively load necessary columns. It is currently being used for handedness information and pc scores
#   The ABCD4.0_MASTER_DATA_FILE is a compilation of data sources. Figure out the actual data sources within the Tabulated BDatasets and Raw Behavioral Data: https://nda.nih.gov/general-query.html?q=query=featured-datasets:Adolescent%20Brain%20Cognitive%20Development%20Study%20(ABCD)
tabulated_data_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD4.0_MASTER_DATA_FILE.csv'
# NDA Dictionary's ABCD Parent Demographics Survey is currently being used for the majority of demographic information. 
nda_dict_demo_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/pdem02.txt'
# NDA Dictionary's ABCD Family History Assessment Part 1 is being used for twin information
nda_dict_twin_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/fhxp102.txt'
# NDA Dictionary's site information path
nda_dict_site_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_lt01.txt'
# NDA Dictionary's anesthesia exposure infromation path 
nda_dict_anes_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_medhxss01.txt'
# MRI Scanner info origin has been changed from this url(https://nda.nih.gov/data_structure.html?short_name=abcd_mri01) to the NDA Dictionary source(ABCD MRI Info)
mri_info_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_mri01.txt'
# QC info origin: https://nda.nih.gov/data_structure.html?short_name=abcd_fastqc01
fastqc01_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_fastqc01-20211221.txt'
# Collection 3165 datastructure manifest origin: https://nda.nih.gov/edit_collection.html?id=3165
c3165_manifest_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_collection-3165-20230407/datastructure_manifest.txt'
# Download previous participants_v1.0.0 from the collection to pull the matched groups. Original origin: https://github.com/DCAN-Labs/automated-subset-analysis
#   TODO: Determine more legitimate source for the matched group info (Box directory with the ABCD 2.0 Release)
original_participants_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/participants_v1.0.0/participants.tsv'

# Hashmap of column name in Tabulated Datasets to that of the participants.tsv
tabulated_data_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "ehi_handedness": "handedness",
    "neurocog_pc1.bl": "pc1",
    "neurocog_pc2.bl": "pc2",
    "neurocog_pc3.bl": "pc3"
}

# Hashmap for the demographic information being used from the NDA Dictionary demographics txt
nda_dict_demo_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "interview_age": "age",
    "demo_sex_v2": "sex",
    "demo_race_a_p___10": "White",
    "demo_race_a_p___11": "Black/African American",
    "demo_race_a_p___12": "American Indian, Native American",
    "demo_race_a_p___13": "Alaska Native",
    "demo_race_a_p___14": "Native Hawaiian",
    "demo_race_a_p___15": "Guamanian",
    "demo_race_a_p___16": "Samoan",
    "demo_race_a_p___17": "Other Pacific Islander",
    "demo_race_a_p___18": "Asian Indian",
    "demo_race_a_p___19": "Chinese",
    "demo_race_a_p___20": "Filipino",
    "demo_race_a_p___21": "Japanese",
    "demo_race_a_p___22": "Korean",
    "demo_race_a_p___23": "Vietnamese",
    "demo_race_a_p___24": "Other Asian",
    "demo_race_a_p___25": "Other Race",
    "demo_race_a_p___77": "Refuse to Answer",
    "demo_race_a_p___99": "Dont Know",
    "demo_ethn_v2": "Do you consider the child Hispanic/Latino/Latina?",
    "demo_comb_income_v2": "income",
    "demo_prnt_ed_v2": "parental_education",
    # !! do we want parental_partner_education information?? !!
    "demo_prtnr_ed_v2": "parental_partner_education",
    "demo_ed_v2": "participant_education"
}

# Hashmap for twin information from the NDA Dictionary's ABCD Family History Assessment Part 1
nda_dict_twin_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "fhx_3c_sibs_same_birth": "siblings_twins"
}

# Hashmap for site information from NDA Dictionary 
nda_dict_site_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "site_id_l": "site"
}

# Hashmap for anesthesia exposure information from NDA Dictionary
nda_dict_anes_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "medhx_ss_9b_p": "anesthesia_exposure"
}

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
# !! Currently not being utilized. Cannot access the linked url above !! 
#pc_map = {
#    "pc1": "neurocog_pc1.bl",
#    "pc2": "neurocog_pc2.bl",
#    "pc3": "eurocog_pc3.bl"
#}

# Hashmap of session variable in the Tabulated Dataset to BIDS format for participants.tsv
bids_session_dict = {
    'baseline_year_1_arm_1': 'ses-baselineYear1Arm1',
    '6_month_follow_up_arm_1': 'ses-6MonthFollowUpArm1',
    '1_year_follow_up_y_arm_1': 'ses-1YearFollowUpYArm1',
    '18_month_follow_up_arm_1': 'ses-18MonthFollowUpArm1',
    '2_year_follow_up_y_arm_1':'ses-2YearFollowUpYArm1',
    '30_month_follow_up_arm_1': 'ses-30MonthFollowUpArm1',
    '3_year_follow_up_y_arm_1': 'ses-3YearFollowUpYArm1',
    '42_month_follow_up_arm_1': 'ses-42MonthFollowUpArm1',
    '4_year_follow_up_y_arm_1': 'ses-4YearFollowUpYArm1',
}

# Based off of sex the child was assigned at birth, may not need this though since its already binarized
#sex_dict = {
#    'M': 1,
#    'F': 2,
#}

# Load the fastqc01.tsv file into a pandas dataframe, skip the second descriptor row, and rename subjectkey to participant_id and visit to session_id
qc_df = pd.read_csv(fastqc01_path, delimiter='\t', skiprows=[1])
# Return a dataframe of all unique subjectkey and visit from the qc_df
qc_subjects = qc_df[['subjectkey', 'visit']].drop_duplicates()
# Rename the subjectkey and visit columns to participant_id and session_id
qc_subjects = qc_subjects.rename(columns={'subjectkey': 'participant_id', 'visit': 'session_id'})

# Load the tabulated data file into a pandas dataframe using the tabulated_data_map keys as the columns
tabulated_data_df = pd.read_csv(tabulated_data_path, usecols=tabulated_data_map.keys()).rename(columns=tabulated_data_map)
# Merge the qc_subjects dataframe with the tabulated_data_df on participant_id and session_id
participants_df = pd.merge(qc_subjects, tabulated_data_df, how='left', on=['participant_id', 'session_id'])

# NEW: Load the proper sex information from the NDA Dictionary demographics source
nda_dict_demo_df = pd.read_csv(nda_dict_demo_path, delimiter='\t', skiprows=[1], usecols=nda_dict_demo_map.keys()).rename(columns=nda_dict_demo_map)
# NEW: Merge the participants_df with the nda_dict_demo_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_demo_df, how='left', on=['participant_id', 'session_id'])

# NEW: Load the proper twin information from the NDA Dictionary's ABCD Family History Assessment Part 1
nda_dict_twin_df = pd.read_csv(nda_dict_twin_path, delimiter='\t', skiprows=[1], usecols=nda_dict_twin_map.keys()).rename(columns=nda_dict_twin_map)
# NEW: Merge the participants_df with the nda_dict_twin_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_twin_df, how='left', on=['participant_id', 'session_id'])

# NEW: Load the proper site information from the NDA Dictionary
nda_dict_site_df = pd.read_csv(nda_dict_site_path, delimiter='\t', skiprows=[1], usecols=nda_dict_site_map.keys()).rename(columns=nda_dict_site_map)
# NEW: Merge the participants_df with the nda_dict_site_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_site_df, how='left', on=['participant_id', 'session_id'])

# NEW: Load the proper anesthesia exposure infromation from the NDA Dictionary
nda_dict_anes_df = pd.read_csv(nda_dict_anes_path, delimiter='\t', skiprows=[1], usecols=nda_dict_anes_map.keys()).rename(columns=nda_dict_anes_map)
# NEW: Merge the participants_df with the nda_dict_anes_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_anes_df, how='left', on=['participant_id', 'session_id'])

# Load the mri_info file into a pandas dataframe using the mri_info_map keys as the columns
mri_info_df = pd.read_csv(mri_info_path, delimiter='\t', usecols=mri_info_map.keys()).rename(columns=mri_info_map)
# Merge the participants_df with the mri_info_df on participant_id and session_id
participants_df = pd.merge(participants_df, mri_info_df, how='left', on=['participant_id', 'session_id'])

# Format the participant_id and session_id to BIDS format
participants_df['participant_id'] = participants_df['participant_id'].apply(lambda x: 'sub-' + x.replace('_',''))
participants_df['session_id'] = participants_df['session_id'].apply(lambda x: bids_session_dict[x])

# Load collection 3165 datastructure manifest and return df of participant_ids and session_ids
c3165_manifest_df = pd.read_csv(c3165_manifest_path, delimiter='\t')

# Write a function to extract the subject and session from the associated_file in c3165_manifest_df
def extract_subject_session(x):
    if re.search('sub-NDARINV[A-Z0-9]{8}/ses-(baselineYear1Arm1|2YearFollowUpYArm1|4YearFollowUpYArm1)', x):
        subject, session = re.search('sub-NDARINV[A-Z0-9]{8}/ses-(baselineYear1Arm1|2YearFollowUpYArm1|4YearFollowUpYArm1)', x).group(0).split('/')
    else:
        subject, session = None, None
    return subject, session

c3165_subject_list = c3165_manifest_df['associated_file'].apply(lambda x: extract_subject_session(x)).tolist()
# return unique elements of c3165_subject_list
c3165_subject_list = list(set(c3165_subject_list))

for x in c3165_subject_list:
    # Set 'collection_3165' to 1 where the participant_id and session_id match x[0] and x[1] in participants_df
    participants_df.loc[(participants_df['participant_id'] == x[0]) & (participants_df['session_id'] == x[1]), 'collection_3165'] = 1

# Load the matched_groups variable from the original participants.tsv into a pandas dataframe
matched_groups_df = pd.read_csv(original_participants_path, delimiter='\t', usecols=['participant_id', 'session_id', 'matched_group'])
participants_df = pd.merge(participants_df, matched_groups_df, how='left', on=['participant_id', 'session_id'])


# !! Get list of all unique mri_info_manufacturer, mri_info_manufacturersmn, and mri_info_softwareversion !!
# participants_df[["mri_info_manufacturer", "mri_info_manufacturersmn", "mri_info_softwareversion"]].drop_duplicates().sort_values(by=["mri_info_manufacturer", "mri_info_manufacturersmn", "mri_info_softwareversion"])


def binarize_race_variables(participants_df):
    #TODO
    # !! convert the value where it matches to a 1 and 0 everywhere else !! is this what we want or should we keep NaN values separate??
    column_header_names = (        
        "White",
        "Black/African American",
        "American Indian, Native American",
        "Alaska Native",
        "Native Hawaiian",
        "Guamanian",
        "Samoan",
        "Other Pacific Islander",
        "Asian Indian",
        "Chinese",
        "Filipino",
        "Japanese",
        "Korean",
        "Vietnamese",
        "Other Asian",
        "Other Race",
        "Refuse to Answer",
        "Dont Know",
        )
    #repeat for all race demographic variables 
    for value in column_header_names:
        participants_df[value] = participants_df[value].fillna(777)
        participants_df[value] = participants_df[value].astype('int')

    return

# !! latinx needs to be converted like the demo variables !!
participants_df["Do you consider the child Hispanic/Latino/Latina?"] = participants_df["Do you consider the child Hispanic/Latino/Latina?"].fillna(0)
participants_df["Do you consider the child Hispanic/Latino/Latina?"] = participants_df["Do you consider the child Hispanic/Latino/Latina?"].astype('int')

binarize_race_variables(participants_df)

#cast handedness as an integer 
participants_df["handedness"] = participants_df["handedness"].fillna(777)
participants_df["handedness"] = participants_df["handedness"].astype('int')
#sibling_twins should be an integer
participants_df["siblings_twins"] = participants_df["siblings_twins"].fillna(777)
participants_df["siblings_twins"] = participants_df["siblings_twins"].astype('int')
#age should be an integer
participants_df["age"] = participants_df["age"].fillna(777)
participants_df["age"] = participants_df["age"].astype('int')
#make sure collection_3165 is an integer
participants_df["collection_3165"] = participants_df["collection_3165"].fillna(0)
participants_df["collection_3165"] = participants_df["collection_3165"].astype('int')

#compare columns with participants.json
#sort from A to Z based on subject ID?

print(participants_df)






#FUTURE: put hash maps into a json format, to make them more readable. Potentially combine them with the data dictionary 

#FUTURE NOTES:

# Data Dictionary Structure
#User
#{
#    [
#        {
#        'column_name': 'participant_id',
#        'data_source': , 
#        'original_name': ,
#        'description': ,
#        }
#    ]
#}
#Programmer
#{
#    'data_source': {
#        'subject_key': 'participant_id',
#    }
#}

# Verify that all required columns exist
#tabulated_datasets_df = pd.read_csv('/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD4.0_MASTER_DATA_FILE.csv')
#for col in tabulated_data_map.keys():
#    print(f'{col} {col in tabulated_datasets_df.columns}')