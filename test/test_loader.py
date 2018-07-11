import unittest
import json
from src.loader import Loader


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.loader = Loader(db_uri='mongodb://localhost:27017/', db_name="test_fever")
        self.coll_purchase = "purchase_detail"
        self.coll_events = "events_info"

    def test_insert_one(self):
        num_before = self.loader.count_elems(self.coll_events)

        self.loader.insert_one(self.coll_events,
                               {"city": "MAD", "event": "end_app_session", "os": "ios", "plan_id": "6846",
                                "time": 1530057600000, "user_id": 611248618,
                                "_id": "611248618|nan|2018-06-27 00:00:00|end_app_session|ios"})

        num_after = self.loader.count_elems(self.coll_events)

        self.assertEqual(num_after, num_before + 1)

    def test_insert_many_one_by_one(self):
        num_before = self.loader.count_elems(self.coll_events)

        with open('/opt/repos/plan-test/test/out/events.json') as f:
            data = json.load(f)

        for json_elem in data:
            self.loader.insert_one(self.coll_events, json_elem)

        num_after = self.loader.count_elems(self.coll_events)

        self.assertEqual(num_after, num_before + len(data))

    def test_insert_many_events(self):
        with open('/opt/repos/plan-test/test/out/events.json') as f:
            data = json.load(f)

        self.loader.insert_many(self.coll_events, data)

    def test_insert_many_purchase(self):
        with open('/opt/repos/plan-test/test/out/purchase.json') as f:
            data = json.load(f)

        self.loader.insert_many(self.coll_events, data)

    def test_delete_many_events(self):
        num_before = self.loader.count_elems(self.coll_events)
        self.assertTrue(0 != num_before)

        self.loader.delete_many(self.coll_events, {})

        num_after = self.loader.count_elems(self.coll_events)
        self.assertEqual(0, num_after)

    def test_delete_many_purchase(self):
        num_before = self.loader.count_elems(self.coll_purchase)
        self.assertTrue(0 != num_before)

        self.loader.delete_many(self.coll_purchase, {})

        num_after = self.loader.count_elems(self.coll_purchase)
        self.assertEqual(0, num_after)


if __name__ == '__main__':
    unittest.main()
