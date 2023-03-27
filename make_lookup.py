#!/usr/bin/env python3

import pandas as pd
import csv

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

lookup_df = pd.DataFrame(columns = ['bids_subject_id', 
                                    'bids_session_id',
                                    'subjectkey',
                                    'src_subject_id',
                                    'interview_date',
                                    'interview_age',
                                    'sex'])

# Additional columns
# neurocog_pc1.bl
# neurocog_pc2.bl
# neurocog_pc3.bl

for index, row in master_df.iterrows():
    bids_subject_id = row['subjectkey'].replace('NDAR_','sub-NDAR')
    bids_session_id = bids_session_dict[row['eventname']]

    lookup_df = lookup_df.append({'bids_subject_id': bids_subject_id, 
                      'bids_session_id': bids_session_id,
                      'subjectkey': row['subjectkey'],
                      'src_subject_id': row['src_subject_id'],
                      'interview_date': row['interview_date'],
                      'interview_age': row['interview_age'],
                      'sex': row['sex']}, ignore_index=True)

lookup_df.to_csv('main_lookup.csv', index=False, quoting=csv.QUOTE_ALL)



