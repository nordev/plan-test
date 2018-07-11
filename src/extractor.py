import json  # Reading db configuration file
import MySQLdb  # Connect to mysql db
import csv  # Import db as csv
import pandas as pd


class Extractor:
    '''
    Esta clase se encarga de conectar con la base de datos MySQL y exportar los datos pertientes.
    '''

    def __init__(self, src_db_config):
        '''
        :param src_db_config: Ruta del fichero con las credenciales de acceso a la Base de Datos.
        '''
        with open(src_db_config) as config:
            db_config = json.load(config)
            db_host = db_config['db_host']
            db_user = db_config['db_user']
            db_password = db_config['db_password']
            db_name = db_config['db_name']

        self.db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_password, db=db_name)
        self.cursor = self.db.cursor()

    def list_data_bases(self):
        '''
        :return: Una lista de string con el nombre de todas las bases de datos en el host al que estamos conectados.
        '''
        databases = ("show databases")
        self.cursor.execute(databases)
        return [x[0] for x in self.cursor.fetchall()]

    def list_tables(self):
        '''
        :return: Una lista de string con el nombre de todas las tablas en la base de datos a la que estamos conectados.
        '''
        self.cursor.execute("SHOW tables")
        return [x[0] for x in self.cursor.fetchall()]

    def set_db(self, db_name):
        '''
        Permite cambiar la conexion a diferentes bases de datos alojadas en el mismo host.
        :param db_name: Nombre de la BD.
        '''
        self.cursor.execute("USE {}".format(db_name))

    def execute(self, query):
        '''
        Ejecuta culaquier codigo SQL.
        :param query: Codigo SQL.
        :return: Cursor con la respuesta.
        '''
        self.cursor.execute(query)
        return self.cursor

    def export_table_to_csv(self, table_name, csv_dst):
        '''
        Extrae el contenido de una tabla y lo escribe en un fichero csv
        :param table_name: Nombre de la tabla para extraer la informacion.
        :param csv_dst: Ruta del csv donde se va a escribir la informacion.
        '''
        self.cursor.execute("SELECT * FROM {}".format(table_name))
        with open(csv_dst, "wb") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in self.cursor.description])
            csv_writer.writerows(self.cursor)

    def export_table_to_df(self, table_name):
        '''
        Extrae el contenido de una tabla y lo escribe en un datframe
        :param table_name: Nombre de la tabla para extraer la informacion.
        :return: Dataframe
        '''
        self.cursor.execute("SELECT * FROM {}".format(table_name))
        header = [i[0] for i in self.cursor.description]
        all_rows = list()
        for row in self.cursor.fetchall():
            curr_row = list()
            for elem in row:
                curr_row.append(elem)
            all_rows.append(curr_row)

        return pd.DataFrame(all_rows, columns=header)

    def close_db(self):
        '''
        Cierra la conexion a la base de datos
        '''
        self.db.close()
