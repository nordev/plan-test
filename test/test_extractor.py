import unittest
from src.extractor import Extractor


class TestExtractor(unittest.TestCase):

    def setUp(self):
        src_db_config = "/opt/repos/plan-test/config/db_config.json"
        self.extractor = Extractor(src_db_config)

    def test_list_data_bases(self):
        res = self.extractor.list_data_bases()
        self.assertTrue("information_schema" in res)
        self.assertTrue("fevertest" in res)

    def test_list_tables(self):
        res = self.extractor.list_tables()
        self.assertTrue("fever_plans" in res)

    def test_execute(self):
        query = "SELECT * FROM fever_plans"
        result = self.extractor.execute(query)
        self.assertEqual(552, result.rowcount)

    def test_export_table_to_csv(self):
        table = "fever_plans"
        csv_dst = "/opt/repos/plan-test/test/out/fever_plans.csv"
        self.extractor.export_table_to_csv(table, csv_dst)

    # def test_export_table_to_df(self):
    #     table = "fever_plans"
    #     df = self.extractor.export_table_to_df(table)
    #     print df.info()


if __name__ == '__main__':
    unittest.main()
