import pandas as pd
import numpy as np
import json


class Transformer:
    def __init__(self):
        pass

    def create_events_info_df(self, src):
        # Set right data type for non string columns
        dtype = {'plan_id': np.int64, 'time': 'datetime64[D]', 'user_id': np.int64}
        df_events = pd.read_json(src, dtype=dtype, encoding='utf-8')
        event_search = ['launch_app', 'start_app_session', 'plan_view', 'tap_on_search_button', 'purchase',
                        'end_app_session']
        # Select rows which column event value is in the list event_search
        df_res = df_events.loc[df_events['event'].isin(event_search)].copy()

        # Delete duplicated lines
        df_res = df_res.drop_duplicates()

        # Create an unique id as primary key for inserting in DB
        df_res['_id'] = df_res.apply(
            lambda row: "{}|{}|{}|{}|{}".format(row['user_id'], row['plan_id'], row['time'], row['event'], row['os']),
            axis=1)
        return df_res

    def create_purchase_detail_df(self, src_json, src_csv):
        df_plans = pd.read_csv(src_csv, dtype={'id': np.int64}, encoding='utf-8')
        df_events = self.create_events_info_df(src_json)

        # Select only events with plan_id
        df_events_fk = df_events.loc[df_events['plan_id'].notna()].copy()

        # Convert again plan_id to int64. Before was an object because there were NaN values
        df_events_fk['plan_id'] = df_events_fk['plan_id'].astype(np.int64)

        # Left join of dfs on primary key plan id.
        df_merged = df_plans.merge(df_events_fk, left_on='id', right_on='plan_id', how='left')

        # Select and rename columns
        df_purchase_detail = df_merged[["user_id", "os", "time", "category", "name"]]
        df_purchase_detail = df_purchase_detail.rename(index=str, columns={"user_id": "user", "time": "purchase_time",
                                                                           "category": "plan_category",
                                                                           "name": "plan_name"})
        df_purchase_detail = df_purchase_detail.drop_duplicates()
        # df_purchase_detail['user'] = df_purchase_detail['user'].astype(np.int64)
        df_purchase_detail['_id'] = df_purchase_detail.apply(
            lambda row: "{}|{}|{}".format(row['user'] if pd.isnull(row['user']) else int(row['user']),
                                          row['purchase_time'], row['plan_name'].encode('utf-8')),
            axis=1)

        return df_purchase_detail

    def get_pct_users_search_before_purchase_tasting(self, src_json, src_csv):
        df_plans = pd.read_csv(src_csv, dtype={'id': np.int64}, encoding='utf-8')
        df_events = self.create_events_info_df(src_json)

        # Select only events with plan_id
        df_events_fk = df_events.loc[df_events['plan_id'].notna()].copy()

        # Convert again plan_id to int64. Before was an object because there were NaN values
        df_events_fk['plan_id'] = df_events_fk['plan_id'].astype(np.int64)

        # Left join of dfs on primary key plan id.
        df_merged = df_plans.merge(df_events_fk, left_on='id', right_on='plan_id', how='left')
        # df_events_fk['plan_id'] = df_events_fk['plan_id'].astype(np.int64)
        # df_events_fk['user_id'] = df_events_fk['user_id'].astype(np.int64)

        user_purchase = df_merged.loc[
            (df_merged['category'] == 'tasting') & (df_merged['event'] == 'purchase')].reset_index()

        # Get max date of every purchase by user
        user_purchase_max = user_purchase[
            user_purchase.groupby('user_id').time.transform('max') == user_purchase['time']]

        # Which of user_purchase_max had done a search before that date
        user_search = df_merged.loc[
            (df_merged['category'] == 'tasting') & (df_merged['event'] == 'plan_view')].reset_index()

        # Get min date of every search by user
        user_search_min = user_search[user_search.groupby('user_id').time.transform('max') == user_search['time']]

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

        # 42.86% of the users have seen the advertiser before make a reservation
        return 100 * float(search_before_purchase_count) / float(user_purchase_count)

    def _write_events_nd_purchase_as_json(self, src_json, src_csv, dst_json, dst_csv):
        df_events = self.create_events_info_df(src_json)
        df_purchase = self.create_purchase_detail_df(src_json, src_csv)

        df_events.to_json(dst_json, orient="records")
        df_purchase.to_json(dst_csv, orient="records")
