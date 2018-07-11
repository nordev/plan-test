from src.extractor import Extractor
from src.transformer import Transformer
from src.loader import Loader
import gzip
import shutil
import pymongo


class Controller:
    '''
    Esta clase se encarga de hacer el proceso ETL llamando a las clases Extractor, Transformer, Loader.
    '''

    def __init__(self):
        self.src_event_compress = "/mnt/fileserver/landing/fever_test_events.json.gz"
        self.src_events = "/opt/repos/plan-test/test/in_files/fever_test_events.json"

        self.src_db_config = "/opt/repos/plan-test/config/db_config.json"
        self.table_plans = "fever_plans"
        self.mongo_db_events = "events_info"
        self.mongo_db_purchase = "purchase_detail"

    def _uncompress_events(self):
        '''
        Descomprime el fichero de eventos desde la carpeta de landing y lo copia a la ruta del proyecto src/in
        :return:
        '''
        # self.src_event_compress = "/home/alvaro/Downloads/fever_test_events_2.json.gz" # Test
        # self.src_events = "/opt/repos/plan-test/src/in/fever_test_events.json" # Test

        with gzip.open(self.src_event_compress, 'rb') as f_in:
            with open(self.src_events, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    def run(self):
        # TODO: Unncomment for uncompress and move events json file compressed
        # self._uncompress_events()

        # TODO: Unncomment for deleting events json file compressed
        # Delete events json file
        # os.remove(src_gz_file)

        extractor = Extractor(self.src_db_config)
        df_plans = extractor.export_table_to_df(self.table_plans)

        transformer = Transformer()
        df_events = transformer.create_events_info_df_from_file(self.src_events)
        df_purchase = transformer.create_purchase_detail_df_from_df(df_events, df_plans)

        loader = Loader(db_name='test_fever')

        events_json = df_events.to_dict(orient="records")
        # loader.delete_many(collection_name=self.mongo_db_events, json_query={}) # Test

        try:
            loader.insert_many(collection_name=self.mongo_db_events, json_list=events_json)
        except pymongo.errors.BulkWriteError:
            loader.upsert_many_one_by_one(collection_name=self.mongo_db_events, json_list=events_json)

        purchase_json = df_purchase.to_dict(orient="records")

        # loader.delete_many(collection_name=self.mongo_db_purchase, json_query={}) # Test
        try:
            loader.insert_many(collection_name=self.mongo_db_purchase, json_list=purchase_json)
        except pymongo.errors.BulkWriteError:
            loader.upsert_many_one_by_one(collection_name=self.mongo_db_purchase, json_list=purchase_json)

        # TODO: Uncomment for deleting event json file
        # os.remove(self.src_events)


if __name__ == '__main__':
    Controller().run()
