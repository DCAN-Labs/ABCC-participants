#!/usr/bin/env python3

import pandas as pd
import re

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# participants.tsv creation process
#   1. Initialize participants.tsv subjects and sessions using the fastqc01 spreadsheet
#   2. Add demographic information from the NDA Dictionary resources and tabulated datasets
#   3. Add MRI information from the mri01 spreadsheet via the NDA Dictionary source
#   4. Add matched group information from the original participants.tsv

# Output path for newest participants.tsv
output_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/participants_v1.0.3/participants.tsv'

# Data Sources

# Download Tabulated Datasets from NDA
#   Do not load this entire file into a pandas dataframe, it will take too much memory instead selectively load necessary columns. It is currently being used for handedness information and pc scores
#   The ABCD4.0_MASTER_DATA_FILE is a compilation of data sources. Figure out the actual data sources within the Tabulated BDatasets and Raw Behavioral Data: https://nda.nih.gov/general-query.html?q=query=featured-datasets:Adolescent%20Brain%20Cognitive%20Development%20Study%20(ABCD)
#   Old source: NeuroCog principal component scores: search for pc1, pc2, and pc3 in https://deap.nimhda.org/applications/Ontology/hierarchy.php?entry=display
tabulated_data_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD4.0_MASTER_DATA_FILE.csv'
# NDA Dictionary's ABCD Parent Demographics Survey is currently being used for demographic information that is invariable
nda_dict_demo_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/pdem02.txt'
# NDA Dictionary's ABCD Longitudinal Parent Demographics Survey contains demographic information that will have changed since baseline
nda_dict_demo_long_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_lpds01.txt'
# NDA Dictionary's ABCD Family History Assessment Part 1 is being used for twin information
nda_dict_twin_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/fhxp102.txt'
# NDA Dictionary's site information path
nda_dict_site_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_lt01.txt'
# NDA Dictionary's longitudial anesthesia exposure infromation path 
nda_dict_anes_long_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_lssmh01.txt'
# NDA Dictionary's baseline anesthesia exposure information path
nda_dict_anes_base_path = '/home/rando149/shared/data/Collection_3165_Supporting_Documentation/abcd_tabulated_data-20230412/abcd_medhxss01.txt'
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

# Hashmap for the demographic information being used from the NDA Dictionary ABCD Parent Demographics Survey that is invariable
nda_dict_demo_map_invariable = {
    "subjectkey": "participant_id",
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
    "demo_race_a_p___99": "Don't Know",
    "demo_ethn_v2": "Do you consider the child Hispanic/Latino/Latina?"
}

# Hashmap for demographic information that is variable but baseline info is only avaible from NDA Dictionary ABCD Parent Demographics Survey
nda_dict_demo_map_baseline = {
    "subjectkey": "participant_id",
    "eventname": "session_id", 
    "demo_comb_income_v2": "income", 
    "demo_prnt_ed_v2": "parental_education_1",
    "demo_prtnr_ed_v2": "parental_partner_education",
    "demo_ed_v2": "participant_education"
}

# Hashmap for the demographic information being used from the Longitudinal Parent Demographics Survey that may change from baseline
nda_dict_demo_long_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "interview_age": "age",
    "demo_ed_v2_l": "participant_education",
    "demo_prnt_ed_v2_l": "parental_education_1",
    "demo_prtnr_ed_v2_l": "parental_partner_education",
    "demo_comb_income_v2_l": "income"
}

# Hashmap for twin information from the NDA Dictionary's ABCD Family History Assessment Part 1
nda_dict_twin_map = {
    "subjectkey": "participant_id",
    "fhx_3c_sibs_same_birth": "siblings_twins"
}

# Hashmap for site information from NDA Dictionary 
nda_dict_site_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "site_id_l": "site"
}

# Hashmap for longitudinal anesthesia exposure information from NDA Dictionary
nda_dict_anes_long_map = {
    "subjectkey": "participant_id",
    "eventname": "session_id",
    "medhx_ss_9b_p_l": "anesthesia_exposure"
}

# Hashmap for baseline anesthesia exposure information from NDA Dictionary
nda_dict_anes_base_map = {
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

# Load the variable demographic information from the NDA Dictionary's ABCD Longitudinal Parent Demographics Survey
nda_dict_demo_long_df = pd.read_csv(nda_dict_demo_long_path, delimiter='\t', skiprows=[1], usecols=nda_dict_demo_long_map.keys()).rename(columns=nda_dict_demo_long_map)
# Merge the participants_df with the nda_dict_demo_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_demo_long_df, how='left', on=['participant_id', 'session_id'])

# Load the invariable demographic information from the NDA Dictionary's ABCD Parent Demographics Survey
nda_dict_demo_df_invariable = pd.read_csv(nda_dict_demo_path, delimiter='\t', skiprows=[1], usecols=nda_dict_demo_map_invariable.keys()).rename(columns=nda_dict_demo_map_invariable)
# Merge the participants_df with the nda_dict_demo_df_invariable on participant_id
participants_df = pd.merge(participants_df, nda_dict_demo_df_invariable, how='left', on=['participant_id'])

# Load the baseline demographic information from the NDA Dictionary's ABCD Parent Demographics Survey
nda_dict_demo_df_baseline = pd.read_csv(nda_dict_demo_path, delimiter='\t', skiprows=[1], usecols=nda_dict_demo_map_baseline.keys()).rename(columns=nda_dict_demo_map_baseline)
# Merge the participants_df with the nda_dict_demo_df_baseline on participant_id
participants_df = pd.merge(participants_df, nda_dict_demo_df_baseline, how='left', on=['participant_id', 'session_id'], suffixes=('', '_new'))
overwrite_columns = ['participant_education', 'parental_education_1', 'parental_partner_education', 'income']
# Update the columns with new values from participants_df (columns with '_new' suffix)
for col in overwrite_columns:
    mask = ~participants_df[col + '_new'].isnull()  # Only update if new value is not NaN
    participants_df.loc[mask, col] = participants_df.loc[mask, col + '_new']
# Drop the columns with '_new' suffix
participants_df.drop([col + '_new' for col in overwrite_columns], axis=1, inplace=True)

# Load the proper twin information from the NDA Dictionary's ABCD Family History Assessment Part 1
nda_dict_twin_df = pd.read_csv(nda_dict_twin_path, delimiter='\t', skiprows=[1], usecols=nda_dict_twin_map.keys()).rename(columns=nda_dict_twin_map)
# Merge the participants_df with the nda_dict_twin_df on participant_id
participants_df = pd.merge(participants_df, nda_dict_twin_df, how='left', on=['participant_id'])

# Load the proper site information from the NDA Dictionary
nda_dict_site_df = pd.read_csv(nda_dict_site_path, delimiter='\t', skiprows=[1], usecols=nda_dict_site_map.keys()).rename(columns=nda_dict_site_map)
# Merge the participants_df with the nda_dict_site_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_site_df, how='left', on=['participant_id', 'session_id'])

# Load the proper longitudinal anesthesia exposure infromation from the NDA Dictionary
nda_dict_anes_long_df = pd.read_csv(nda_dict_anes_long_path, delimiter='\t', skiprows=[1], usecols=nda_dict_anes_long_map.keys()).rename(columns=nda_dict_anes_long_map)
# Merge the participants_df with the nda_dict_anes_long_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_anes_long_df, how='left', on=['participant_id', 'session_id'])

# Load the proper baseline anesthesia exposure information from the NDA Dictionary
nda_dict_anes_base_df = pd.read_csv(nda_dict_anes_base_path, delimiter='\t', skiprows=[1], usecols=nda_dict_anes_base_map.keys()).rename(columns=nda_dict_anes_base_map)
# Merge the participants_df with the nda_dict_anes_base_df on participant_id and session_id
participants_df = pd.merge(participants_df, nda_dict_anes_base_df, how='left', on=['participant_id', 'session_id'], suffixes=('', '_new'))
# Update the column with new values from participants_df (column with '_new' suffix)
mask2 = ~participants_df['anesthesia_exposure_new'].isnull() # Only update if new value is not NaN
participants_df.loc[mask2, 'anesthesia_exposure'] = participants_df.loc[mask2, 'anesthesia_exposure_new']
# Drop the column with the '_new' suffix
participants_df.drop(['anesthesia_exposure_new'], axis=1, inplace=True)

# Load the mri_info file into a pandas dataframe using the mri_info_map keys as the columns
mri_info_df = pd.read_csv(mri_info_path, delimiter='\t', skiprows=[1], usecols=mri_info_map.keys()).rename(columns=mri_info_map)
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
# Return unique elements of c3165_subject_list
c3165_subject_list = list(set(c3165_subject_list))

for x in c3165_subject_list:
    # Set 'collection_3165' to 1 where the participant_id and session_id match x[0] and x[1] in participants_df
    participants_df.loc[(participants_df['participant_id'] == x[0]) & (participants_df['session_id'] == x[1]), 'collection_3165'] = 1

# Load the matched_groups variable from the original participants.tsv into a pandas dataframe and drop dupilcate participant ids
matched_groups_df = pd.read_csv(original_participants_path, delimiter='\t', usecols=['participant_id', 'matched_group']) 
matched_groups_df_unique = matched_groups_df[['participant_id', 'matched_group']].drop_duplicates() 
participants_df = pd.merge(participants_df, matched_groups_df_unique, how='left', on=['participant_id'])

# Function to properly format each race information column
def race_column_info(participants_df):
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
        "Don't Know",
        )
    # Repeat for all race demographic columns 
    for value in column_header_names:
        participants_df[value] = participants_df[value].fillna(888)
        participants_df[value] = participants_df[value].astype('int')

    return

# Convert latinx variable the same way as the other race columns
participants_df["Do you consider the child Hispanic/Latino/Latina?"] = participants_df["Do you consider the child Hispanic/Latino/Latina?"].fillna(888)
participants_df["Do you consider the child Hispanic/Latino/Latina?"] = participants_df["Do you consider the child Hispanic/Latino/Latina?"].astype('int')

race_column_info(participants_df)

# Cast handedness as an integer and fill in NaNs with 888
participants_df["handedness"] = participants_df["handedness"].fillna(888)
participants_df["handedness"] = participants_df["handedness"].astype('int')

# Cast sibling_twins as an integer and fill in NaNs with 888
participants_df["siblings_twins"] = participants_df["siblings_twins"].fillna(888)
participants_df["siblings_twins"] = participants_df["siblings_twins"].astype('int')

# Cast age as an integer and fill in NaNs with 888
participants_df["age"] = participants_df["age"].fillna(888)
participants_df["age"] = participants_df["age"].astype('int')

# Cast collection_3165 as an integer and fill in NaNs with 0
participants_df["collection_3165"] = participants_df["collection_3165"].fillna(0)
participants_df["collection_3165"] = participants_df["collection_3165"].astype('int')

# Fill site NaNs with integer 888
participants_df["site"] = participants_df["site"].fillna(888)

# Fill scanner_manufacturer NaNs with integer 888
participants_df["scanner_manufacturer"] = participants_df["scanner_manufacturer"].fillna(888)

# Fill scanner_model NaNs with integer 888
participants_df["scanner_model"] = participants_df["scanner_model"].fillna(888)

# Rename scanner_software values to match previous participants.tsv fill NaNs with 888 
participants_df["scanner_software"] = participants_df["scanner_software"].fillna(888)
scanner_software_dict = {
    "syngo MR E11": "syngo MR E11",
    "5.3.05.3.0.0": "[5.3.0, 5.3.0.0]",
    "5.3.05.3.0.3": "[5.3.0, 5.3.0.3]",
    "5.3.15.3.1.0": "[5.3.1, 5.3.1.0]",
    "5.3.15.3.1.1": "[5.3.1, 5.3.1.1]",
    "5.3.15.3.1.2": "[5.3.1, 5.3.1.2]",
    "5.3.15.3.1.3": "[5.3.1, 5.3.1.3]",
    "5.4.05.4.0.1": "[5.4.0, 5.4.0.1]",
    "5.4.15.4.1.1": "[5.4.1, 5.4.1.1]",
    "5.6.15.6.1.1": "[5.6.1, 5.6.1.1]",
    "25LXMR Software release:DV25.0_R02_1549.b": "[25, LX, MR Software release:DV25.0_R02_1549.b]",
    "27LXMR Software release:DV25.1_R01_1617.b": "[27, LX, MR Software release:DV25.1_R01_1617.b]",
    "27LXMR Software release:DV26.0_EB_1707.b": "[27, LX, MR Software release:DV26.0_EB_1707.b]",
    "27LXMR Software release:DV26.0_R01_1725.a": "[27, LX, MR Software release:DV26.0_R01_1725.a]",
    "27LXMR Software release:DV26.0_R02_1810.b": "[27, LX, MR Software release:DV26.0_R02_1810.b]",
    "27LXMR Software release:DV26.0_R03_1831.b": "[27, LX, MR Software release:DV26.0_R03_1831.b]",
    "27LXMR Software release:DV26.0_R05_2008.a": "[27, LX, MR Software release:DV26.0_R05_2008.a]",
    "27Orchestra SDK": "[27, Orchestra SDK]",
    888: 888
}
participants_df['scanner_software'] = participants_df['scanner_software'].apply(lambda x: scanner_software_dict[x])

# Cast matched_group as an integer and fill in NaNs with 888
participants_df["matched_group"] = participants_df["matched_group"].fillna(888)
participants_df["matched_group"] = participants_df["matched_group"].astype('int')

# Cast sex information as an integer and fill in NaNs with 888
participants_df["sex"] = participants_df["sex"].fillna(888)
participants_df["sex"] = participants_df["sex"].astype('int')

# Cast income as an integer and fill in NaNs with 888
participants_df["income"] = participants_df["income"].fillna(888)
participants_df["income"] = participants_df["income"].astype('int')

# Cast participant_education as an integer and fill in NaNs with 888
participants_df["participant_education"] = participants_df["participant_education"].fillna(888)
participants_df["participant_education"] = participants_df["participant_education"].astype('int')

# Calculate max between parental_education_1 and parental_partner_education and save in new parental_education variable, then cast as integer and fill in NaNs with 888
participants_df["parental_education_1"] = participants_df["parental_education_1"].replace({999.0: -0.1, 777.0: -0.2})
participants_df["parental_partner_education"] = participants_df["parental_partner_education"].replace({999.0: -0.1, 777.0: -0.2})
participants_df["parental_education"] = participants_df.apply(lambda row: max(row["parental_education_1"], row["parental_partner_education"]), axis=1)
participants_df["parental_education"] = participants_df["parental_education"].replace({-0.1: 999.0, -0.2: 777.0})
participants_df["parental_education"] = participants_df["parental_education"].fillna(888)
participants_df["parental_education"] = participants_df["parental_education"].astype('int')

# Cast anesthesia_exposure as an integer and fill in NaNs with 888
participants_df["anesthesia_exposure"] = participants_df["anesthesia_exposure"].fillna(888)
participants_df["anesthesia_exposure"] = participants_df["anesthesia_exposure"].astype('int')

# Fill in pc scores' NaNs with 888
participants_df["pc1"] = participants_df["pc1"].fillna(888)
participants_df["pc2"] = participants_df["pc2"].fillna(888)
participants_df["pc3"] = participants_df["pc3"].fillna(888)

# Reorder columns to match older versions of participants tsv
reordered_columns = [
    "participant_id", 
    "session_id", 
    "collection_3165", 
    "site", 
    "scanner_manufacturer", 
    "scanner_model", 
    "scanner_software", 
    "matched_group", 
    "sex", 
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
    "Don't Know", 
    "Do you consider the child Hispanic/Latino/Latina?",
    "age", 
    "handedness", 
    "siblings_twins", 
    "income", 
    "participant_education", 
    "parental_education", 
    "anesthesia_exposure",
    "pc1",
    "pc2",
    "pc3"
    ] 
participants_df_reordered = participants_df[reordered_columns]

# Sort rows A to Z by participant_id
participants_df_reordered_sorted = participants_df_reordered.sort_values(by='participant_id')

# Convert participants_df_reordered_sorted to tsv format
participants_df_reordered_sorted.to_csv(output_path, sep='\t', index=False)

#FUTURE NOTES:
#
# put hash maps into a json format, to make them more readable. Potentially combine them with the data dictionary 
# 
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
# 
# Verify that all required columns exist
#tabulated_datasets_df = pd.read_csv('/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD4.0_MASTER_DATA_FILE.csv')
#for col in tabulated_data_map.keys():
#    print(f'{col} {col in tabulated_datasets_df.columns}')