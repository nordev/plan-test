import unittest

from src.transformer import Transformer


class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = Transformer()
        self.src_json = "/opt/repos/plan-test/test/in_files/fever_test_events.json"
        self.src_csv = "/opt/repos/plan-test/test/in_files/fever_plans.csv"

    def test_create_events_info_df(self):
        res = self.transformer.create_events_info_df(self.src_json)
        self.assertEqual(42418,len(res))


    def test_create_purchase_detail_df(self):
        res = self.transformer.create_purchase_detail_df(self.src_json, self.src_csv)
        self.assertEqual(13590,len(res))

    # def test_get_pct_users_search_before_purchase_tasting(self):
    #     print self.transformer.get_pct_users_search_before_purchase_tasting(self.src_json, self.src_csv)

    def test__write_events_nd_purchase_as_json(self):
        dst_json = "/opt/repos/plan-test/test/out/events.json"
        dst_csv = "/opt/repos/plan-test/test/out/purchase.json"
        self.transformer._write_events_nd_purchase_as_json(self.src_json,self.src_csv,dst_json,dst_csv)

if __name__ == '__main__':
    unittest.main()
