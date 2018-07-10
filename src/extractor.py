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
        return self.cursor.execute(databases)

    def list_tables(self):
        return self.cursor.execute("SHOW tables")

    def set_db(self, db_name):
        self.cursor.execute("USE {}".format(db_name))

    def execute(self, query):
        return self.cursor.execute(query)

    def export_table(self, table_name, csv_dst):
        cur = self.cursor.execute("SELECT * FROM {}".format(table_name))
        with open(csv_dst, "wb") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in cur.description])
            csv_writer.writerows(cur)

    def close_db(self):
        self.db.close()
