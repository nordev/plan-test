# Comandos utilizados para la instalación del software de la aplicación.

# updates the package lists 
sudo apt update

# Instalar Python 2.7
sudo apt install python2.7

# Instalar pip para las librerías Python que nombramos abajo
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py

# Instalar dependencias para librería de python mysqlclient
sudo apt install python-dev libmysqlclient-dev
sudo pip install mysqlclient

# Instalar pandas para la lectura y transformación de datos
sudo pip install pandas


# Instalar mongoDB
1 - Importar una clave publica para poder descargar el paquete
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4

2- Crear una lista de ficheros para mongo db
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/4.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.0.list

3 - Instalar el paquete
sudo apt-get update
sudo apt-get install -y mongodb-org

4 - Arrancar el servicio
sudo service mongod start

5 - Comprobar que el servidor acepta peticiones (debes ver waiting for connections on port 27017)
cat /var/log/mongodb/mongod.log | grep waiting

6 - Arrancar el servidor
mongo --host 127.0.0.1:27017

# Prepare repo.
sudo mkdir /opt/repos
chown ubuntu:ubuntu repos/
cd /opt/repos/
git clone https://github.com/nordev/plan-test.git

# Fijar la variable de entorno PythonPath con el valor de la raiz del repo.
export PYTHONPATH=/opt/repos/plan-test/

python -m unittest test_extractor.TestExtractor

# Execute test with
sh /opt/repos/plan-test/test/execute_test.sh

# Introducción
Esta aplicación pretende emular un pipeline de ETL. De acuerdo a esta idea se han creado tres clases: Extractor, Transformer y Loader.

La clase Extractor es un wrapper del conector MySQLdb y ofrece la funcionalidad necesaria para conectarse a una base de datos mysql, explorarla y exportar a csv la información. La exportación a de las tablas de la BD se podría haber hecho de varias maneras. Haciendo un dump de la base de datos.O cargando las tablas directamente en memoria. Quizás la exportación a csv es la menos eficiente pero permite mayor flexibilidad a la hora de la implementación.

# Decisiones de diseño
A pesar de que una gran cantidad de registros de eventos no contienen un plan_id se almacenan en BD porque así lo sugiere el enunciado.

Hablar de data cleaning IOS ios

Hablar de la poca eficiencia de guardar las claves as string

Los json se pueden insertar todos a la vez.
De uno en uno 
En bloques de 1

# 

# Before start you should install MongoDB
# sudo service mongod start
# cat /var/log/mongodb/mongod.log (find the line Waiting for connections on port 27017)
# Type in console: mongo --host 127.0.0.1:27017

# plan-test
A test about Python and other technologies

# To install 





