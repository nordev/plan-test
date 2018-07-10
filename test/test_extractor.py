import  unittest
from src.extractor import Extractor


class TestExtractor(unittest.TestCase):

    def setUp(self):
        src_db_config = "../config/db_config.json"
        self.extractor = Extractor(src_db_config)

    def tearDown(self):
        pass

    def test_list_data_bases(self):
        cursor_res = self.extractor.list_data_bases()
        for (databases) in cursor_res:
            print databases[0]
            # Result:
            # information_schema
            # fevertest
            # mysql
            # performance_schema
            # sys

    def test_list_tables(self):
        cursor_res = self.extractor.list_tables()
        for (tables) in cursor_res:
            print tables[0]
            # Result:
            # fever_plans

    def test_set_db(self):
        db_name = "fevertest"
        self.extractor.set_db(db_name)

    def test_execute(self):
        query = "SELECT * FROM fever_plans"
        result = self.extractor.execute(query)
        self.assertEqual(552, result.rowcount)

    def test_export_table(self):
        table = "fever_plans"
        csv_dst = "/home/ubuntu/fever_plans.csv"
        self.extractor.export_table(table, csv_dst)

if __name__ == '__main__':
    unittest.main()