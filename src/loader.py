from pymongo import MongoClient


class Loader:

    def __init__(self, db_name, db_uri='mongodb://localhost:27017/'):
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def insert_one(self, collection_name, json_elem):
        collection = self.get_collection(collection_name)
        return collection.insert_one(json_elem)

    def insert_many(self, collection_name, json_list):
        collection = self.get_collection(collection_name)
        return collection.insert_many(json_list)


    def delete_many(self, collection_name, json_query=None):
        collection = self.get_collection(collection_name)
        return collection.delete_many(json_query)

    def count_elems(self, collection_name):
        collection = self.get_collection(collection_name)
        return collection.count()