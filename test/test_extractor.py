import  unittest
from src.extractor import Extractor


class TestExtractor(unittest.TestCase):

    def setUp(self):
        src_db_config = "../config/db_config.json"
        self.extractor = Extractor(src_db_config)

    def tearDown(self):
        pass

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

    def test_export_table(self):
        table = "fever_plans"
        csv_dst = "./out/fever_plans.csv"
        self.extractor.export_table(table, csv_dst)

if __name__ == '__main__':
    unittest.main()