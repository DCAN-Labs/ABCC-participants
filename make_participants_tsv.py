#!/usr/bin/env python3

import pandas as pd
import numpy as np
import csv

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

master_df = pd.read_csv('/home/rando149/shared/data/Collection_3165_Supporting_Documentation/ABCD2.0_MASTER_DATA_FILE_2.2.22_reduced.csv')

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

# master_df.columns =
# Index(['Unnamed: 0', 'subjectid', 'src_subject_id', 'subjectkey',
#       'interview_date', 'interview_age', 'sex', 'eventname', 'site_id_l',
#       'abcd_site', 'age', 'female', 'married.or.livingtogether.bl',
#       'married.bl', 'high.educ.bl', 'hisp', 'event_name',
#       'interview_datetime', 'sex_at_birth', 'genetic_paired_subjectid_1',
#       'genetic_paired_subjectid_2', 'genetic_paired_subjectid_3',
#       'genetic_paired_subjectid_4'],
#      dtype='object')

demo_race_a_p___10 = White
demo_race_a_p___11 = Black/African American
demo_race_a_p___12 = American Indian, Native American
demo_race_a_p___13 = Alaska Native
demo_race_a_p___14 = Native Hawaiian
demo_race_a_p___15 = Guamanian
demo_race_a_p___16 = Samoan
demo_race_a_p___17 = Other Pacific Islander
demo_race_a_p___18 = Asian Indian
demo_race_a_p___19 = Chinese
demo_race_a_p___20 = Filipino
demo_race_a_p___21 = Japanese
demo_race_a_p___22 = Korean
demo_race_a_p___23 = Vietnamese
demo_race_a_p___24 = Other Asian
demo_race_a_p___25 = Other Race
demo_race_a_p___77 = Refuse to Answer
demo_race_a_p___99 = Dont Know

demo_ethn_p = Hispanic/Latino/Latina

demo_prnt_ed_v2 = Highest grade or level of school you have completed or the highest degree you have received (From LORIS)
demo_prnt_ed_p_l and demo_prnt_ed_p in master data file


column_map = {
    "participant_id": "src_subject_id",
    "session_id": "eventname",
    "collection_3165": "1",
    "site": "site_id_l"
    "scanner_manufacturer": ""
    "scanner_model": ""
    "scanner_software": ""
    "matched_group": ""
    "sex": "sex"
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
    "age": "age"
    "handedness": "ehi_handedness"
    "siblings_twins": "devhx_5_twin_p"
    "income": "demo_comb_income_p_l"
    "participant_education": "demo_ed_p_l'"
    "parental_education": "demo_prnt_ed_p_l"
    "anesthesia_exposure": "medhx_9a_anesthesia_p_l "
    "pc1": ""
    "pc2": ""
    "pc3": ""
    "latinx": "demo_ethn_p"
}
#TODO: Validate that similiarly named columns are identical and if they are not then investigate the difference
#   'subjectid', 'src_subject_id', 'subjectkey'
#   'site_id_l' or 'abcd_site'
#   'sex' or 'sex_at_birth'
#   demo_comb_income_p_l, demo_comb_income_p
#


old_participants_df = pd.read_csv('/home/rando149/shared/data/Collection_3165_Supporting_Documentation/participants_v1.0.2/participants.tsv', delimiter='\t')

#participants_df.columns = 
#Index(['participant_id', 'session_id', 'collection_3165', 'site',
#       'scanner_manufacturer', 'scanner_model', 'scanner_software',
#       'matched_group', 'sex', 'race_ethnicity', 'age', 'handedness',
#       'siblings_twins', 'income', 'participant_education',
#       'parental_education', 'anesthesia_exposure'],
#      dtype='object')

new_participants_df = pd.DataFrame()

for index, row in master_df.iterrows():
    bids_subject_id = row['subjectkey'].replace('NDAR_','sub-NDAR')
    bids_session_id = bids_session_dict[row['eventname']]

    # Locate the given subject and session in the old participants_df
    subject_df = old_participants_df.copy()[(old_participants_df['participant_id'] == bids_subject_id) & (old_participants_df['session_id'] == bids_session_id)]
    if subject_df.empty:
        continue
    elif len(subject_df) == 2:
        print("WARNING: Duplicate entries for subjects {} {}".format(bids_subject_id, bids_session_id))
        # Highlight discrepencies between entries
        diff = subject_df.iloc[0][subject_df.iloc[0].ne(subject_df.iloc[1], axis=0)]
        print(diff)
        try:
            diff.index.tolist() == ['site']
        except:
            print("ERROR: Further error handling required")
        subject_df = subject_df.iloc[0]
    elif len(subject_df) > 2:
        print("WARNING: Multiple entries for subjects {} {}".format(bids_subject_id, bids_session_id))
        print(subject_df)
        subject_df = subject_df.iloc[0]
    else:
        subject_df = subject_df.iloc[0]

    # Check site
    master_site = row['site_id_l']
    if master_site == np.nan:
        master_site = '888'
    if subject_df['site'] != master_site:
        print("Replace {} with {} for {}".format(subject_df['site'], master_site, bids_subject_id))
        subject_df['site'] = master_site

    # Check sex
    master_sex = row['sex']
    if master_sex == np.nan:
        master_sex = '888'
    if subject_df['sex'] != sex_dict[master_sex]:
        print("Replace {} with {} for {}".format(subject_df['sex'], sex_dict[master_sex], bids_subject_id))
        subject_df['sex'] = sex_dict[master_sex]

    # Make new corrected participants_df
    new_participants_df = new_participants_df.append(subject_df)

# Fix types of numerical columns
for col in new_participants_df.columns:
    if new_participants_df[col].dtypes == 'float64':
        new_participants_df[col] = new_participants_df[col].astype(int)

# Order column names
new_participants_df = new_participants_df[old_participants_df.columns]

print(new_participants_df)
print('Done writing new participants.tsv file')
new_participants_df.to_csv('/home/rando149/shared/data/Collection_3165_Supporting_Documentation/new_participants.tsv', index=False, sep='\t', quoting=csv.QUOTE_ALL)



