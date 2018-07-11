# This script want to answer the question:
# What percentage of users that purchase in the tasting category have
# performed a search in the app prior to the purchase event?

import numpy as np
import pandas as pd
from src.transformer import Transformer

src_csv = "/opt/repos/plan-test/src/in/fever_plans.csv"
src_json = "/opt/repos/plan-test/src/in/fever_test_events.json"

df_plans = pd.read_csv(src_csv, dtype={'id': np.int64}, encoding='utf-8')
df_events = Transformer().create_events_info_df_from_file(src_json)

# Select only events with plan_id
df_events_fk = df_events.loc[df_events['plan_id'].notna()].copy()

# Convert again plan_id to int64. Before was an object because there were NaN values
df_events_fk['plan_id'] = df_events_fk['plan_id'].astype(np.int64)

# Left join of dfs on primary key plan id.
df_merged = df_plans.merge(df_events_fk, left_on='id', right_on='plan_id', how='left')

user_purchase = df_merged.loc[
    (df_merged['category'] == 'tasting') & (df_merged['event'] == 'purchase')].reset_index()

# Get max date of every purchase by user
user_purchase_max = user_purchase[
    user_purchase.groupby('user_id').time.transform('max') == user_purchase['time']]

# Which of user_purchase_max had done a search before that date
user_search = df_merged.loc[
    (df_merged['category'] == 'tasting') & (df_merged['event'] == 'plan_view')].reset_index()

# Get min date of every search by user
user_search_min = user_search[user_search.groupby('user_id').time.transform('min') == user_search['time']]

# Join users that purchase and user that search by user_id and plan id
purchase_search_merged = user_purchase_max.merge(user_search_min, left_on=['user_id', 'id'],
                                                 right_on=['user_id', 'id'], how='inner',
                                                 suffixes=["_purchase", "_search"])

# Select user that have searched a plan before purchase it
purchase_search_merged['is_lesser'] = purchase_search_merged.apply(
    lambda row: row['time_search'] < row['time_purchase'], axis=1)

purchase_search_merged_ft = purchase_search_merged[["user_id", "id", "time_purchase", "time_search"]].loc[
    purchase_search_merged['is_lesser']]

# Number of user that purchased in the category tasting
user_purchase_count = user_purchase['user_id'].unique().shape[0]

# Number of user that had searched before purchase the same plan in the category tasting
search_before_purchase_count = purchase_search_merged_ft["user_id"].unique().shape[0]

# 8.49% of the users have seen the advertiser SAME DAY that did a reservation
result = 100 * float(search_before_purchase_count) / float(user_purchase_count)
print result
