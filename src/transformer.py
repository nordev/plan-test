import pandas as pd
import numpy as np
import json


class Transformer:
    def __init__(self):
        pass

    def create_events_info_df_from_file(self, src):
        '''
        Filtra un fichero json con eventos en base a las categorias estipuladas. Se eliminan las lineas duplicadas. Y le
        otorga una clave unica para su posterior almacenamiento.
        :param src: Ruta del fichero json de eventos
        :return: DataFrame con los eventos filtrados
        '''
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

    def create_purchase_detail_df_from_df(self, df_events, df_plans):
        '''
        Crea un DataFrame con los detalles de compra. Para ello cruza los DataFrames de eventos y planes,
         renombra sus columnas y elimina duplicados.
        :param df_events: DataFrame con eventos
        :param df_plans: DataFrame con planes
        :return: Dataframe con detalles de compra
        '''

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

        df_purchase_detail['_id'] = df_purchase_detail.apply(
            lambda row: "{}|{}|{}".format(row['user'] if pd.isnull(row['user']) else int(row['user']),
                                          row['purchase_time'], row['plan_name']),
            axis=1)

        # Hack for later storing in db. MongoDB doesnot accept pandas type 'NaT'. This value appear when there is an
        # empty date
        df_purchase_detail['purchase_time'] = df_purchase_detail.apply(lambda row: "{}".format(
            str(row['purchase_time']) if row['purchase_time'] == np.datetime64('NaT') else row['purchase_time']),
                                                                       axis=1)

        return df_purchase_detail

    def create_purchase_detail_df_from_file(self, src_events, src_plans):
        '''
        Idem a la funcion create_purchase_detail_df_from_df pero obtiene como entrada ficheros
        :param src_events: Ruta al fichero con los eventos
        :param src_plans: Ruta al ffichero con los planes
        :return: DataFrame con los detalles de compra
        '''
        df_plans = pd.read_csv(src_plans, dtype={'id': np.int64}, encoding='utf-8')
        return self.create_purchase_detail_df_from_df(src_events, df_plans)

    def _write_events_nd_purchase_as_json(self, src_events, src_plans, dst_events, dst_plans):
        '''

        :param src_events: La ruta origen del fichero de eventos
        :param src_plans: La ruta origen del fichero de planes
        :param dst_events: La ruta destino del fichero de planes
        :param dst_plans: La ruta destino del fichero de planes
        '''
        df_events = self.create_events_info_df_from_file(src_events)
        df_purchase = self.create_purchase_detail_df_from_file(src_events, src_plans)

        df_events.to_json(dst_events, orient="records")
        df_purchase.to_json(dst_plans, orient="records")
