import unittest
import json
from src.loader import Loader


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.loader = Loader(db_uri='mongodb://localhost:27017/', db_name="test_fever_unittest")
        self.coll_purchase = "purchase_detail"
        self.coll_events = "events_info"

    def tearDown(self):
        self.loader.delete_many(self.coll_purchase, {})
        self.loader.delete_many(self.coll_events, {})

    def test_insert_many_events(self):
        with open('/opt/repos/plan-test/test/out/events.json') as f:
            data = json.load(f)

        self.loader.insert_many(self.coll_events, data)

    def test_insert_many_purchase(self):
        with open('/opt/repos/plan-test/test/out/purchase.json') as f:
            data = json.load(f)

        self.loader.insert_many(self.coll_events, data)

    def test_insert_one(self):
        num_before = self.loader.count_elems(self.coll_events)

        self.loader.insert_one(self.coll_events,
                               {"city": "MAD", "event": "end_app_session", "os": "ios", "plan_id": "6846",
                                "time": 1530057600000, "user_id": 611248618,
                                "_id": "611248618|nan|2018-06-27 00:00:00|end_app_session|ios"})

        num_after = self.loader.count_elems(self.coll_events)

        self.assertEqual(num_after, num_before + 1)

    # def test_insert_many_one_by_one(self):
    #     num_before = self.loader.count_elems(self.coll_events)
    #
    #     with open('/opt/repos/plan-test/test/out/events.json') as f:
    #         data = json.load(f)
    #
    #     for json_elem in data:
    #         self.loader.insert_one(self.coll_events, json_elem)
    #
    #     num_after = self.loader.count_elems(self.coll_events)
    #
    #     self.assertEqual(num_after, num_before + len(data))

    # def test_upsert_many_one_by_one(self):
    #     jsons = [{"city": "MAD", "event": "end_app_session", "os": "ios", "plan_id": "6846",
    #      "time": 1530057600000, "user_id": 611248618,
    #      "_id": "611248618|nan|2018-06-27 00:00:00|end_app_session|ios"},
    #
    #     {"city": "MAD", "event": "end_app_session", "os": "ios", "plan_id": "6846",
    #      "time": 1530057600000, "user_id": 611248618,
    #      "_id": "ABC|nan|2018-06-27 00:00:00|end_app_session|ios"}]
    #
    #     collection = self.loader.get_collection(self.coll_events)
    #     for document in jsons:
    #         id_mongo = document['_id']
    #         collection.update_one(filter={'_id': id_mongo}, update={"$set": document},upsert=True)


if __name__ == '__main__':
    unittest.main()
