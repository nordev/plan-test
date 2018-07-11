from pymongo import MongoClient


class Loader:
    '''
    Esta clase se encarga de recibir datos en formato json y subirlos a una base de datos MongoDB.
    '''

    def __init__(self, db_name, db_uri='mongodb://localhost:27017/'):
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        '''
        Devuelve una coleccion de MongoDB -equivalente a una tabla sql-.
        :param collection_name:
        :return: Un cursor a la coleccion
        '''
        return self.db[collection_name]

    def insert_one(self, collection_name, json_elem):
        '''
        Inserta un documento -equivalente a una file en sql-.
        :param collection_name:
        :param json_elem: Un diccionario o json con los datos del documento.
        :return:  objeto InsertOneResult permite ver el id del documento insertado.
        '''
        collection = self.get_collection(collection_name)
        return collection.insert_one(json_elem)

    def insert_many(self, collection_name, json_list):
        '''
        Permite insertar una lista de diccionarios o jsons en batch.
        :param collection_name:
        :param json_list: Lista de diccionarios o jsons.
        :return: objeto InsertManyResult permite ver los ids del documento insertado.
        '''
        collection = self.get_collection(collection_name)
        return collection.insert_many(json_list)

    def upsert_many_one_by_one(self, collection_name, json_list):
        '''
        Permite insertar una lista de diccionarios o jsons uno por uno. Tiene la particularidad de que actualiza los
        registros existentes e inserta los que no existen.
        :param collection_name:
        :param json_list:
        '''
        collection = self.get_collection(collection_name)
        for document in json_list:
            id_mongo = document['_id']
            collection.update_one(filter={'_id': id_mongo}, update={"$set": document}, upsert=True)

    def delete_many(self, collection_name, json_query=None):
        '''
        Borra docuementos de una coleccion en base a un filtro.
        :param collection_name:
        :param json_query: Filtro en forma de quey utilizado
        :return:
        '''
        collection = self.get_collection(collection_name)
        return collection.delete_many(json_query)

    def count_elems(self, collection_name):
        '''
        Cuenta el numero de elementos que hay en una coleccion.
        :param collection_name:
        :return: Entero
        '''
        collection = self.get_collection(collection_name)
        return collection.count()
