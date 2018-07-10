import json  # Reading db configuration file
import MySQLdb  # Connect to mysql db
import csv  # Import db as csv


class Extractor:

    def __init__(self, src_db_config):
        with open(src_db_config) as config:
            db_config = json.load(config)
            db_host = db_config['db_host']
            db_user = db_config['db_user']
            db_password = db_config['db_password']
            db_name = db_config['db_name']

        self.db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_password, db=db_name)
        self.cursor = self.db.cursor()

    def list_data_bases(self):
        databases = ("show databases")
        self.cursor.execute(databases)
        return [x[0] for x in self.cursor.fetchall()]

    def list_tables(self):
        self.cursor.execute("SHOW tables")
        return [x[0] for x in self.cursor.fetchall()]

    def set_db(self, db_name):
        self.cursor.execute("USE {}".format(db_name))

    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor

    def export_table(self, table_name, csv_dst):
        self.cursor.execute("SELECT * FROM {}".format(table_name))
        with open(csv_dst, "wb") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in self.cursor.description])
            csv_writer.writerows(self.cursor)

    def close_db(self):
        self.db.close()
