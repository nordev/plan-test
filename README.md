## ¿Qué es esto?

**plan-test** es una aplicación de juguete escrita en Python y pretende simular un ETL real y 
su arquitectura. La aplicación cuenta con un proceso de extracción en MySQL a través de python.
 También hay un proceso de transformación y limpieza de datos haciendo uso de la librería de Python
  Pandas. También se implementan las operaciones CRUD sobre una base de datos Mongo utilizando 
  la librería de Python PyMongo.

**Esta aplicación ha sido programada en menos de dos días y solo es un prototipo**.

## ¿Cómo ejecutar la aplicación?
La aplicación se ejecutará todos los días a las 6 de la mañana. Para cumplir este requisito se ha utilizado la herramient crontab. Notar que existen herramientas más completas para hacer estos procedimientos como Airflow que proporciona una interfaz gráfica basada en DAG (Grafos Asíncronos Dirigídos) para visualizar este tipo de procedimientos.
```sh
crontab -e
0 6 * * * python /opt/repos/plan-test/src/controller.py >/dev/null 2>&1
```

Si prefieres ejecutar la aplicación de forma manual es suficiente con:
```sh
python /opt/repos/plan-test/src/controller.py
```

Si quieres ejecutar los test unitarios escribe:
```sh
cd /opt/repos/plan-test/test
sh execute_test.sh
```
**Antes de ejecutar la aplicación debes cumplir con los siguientes requisitos.**

## Prerrequisitos

###Instalar python y pip
```sh
# actualizar lista de paquetes
sudo apt update
```

```sh
# Instalar Python 2.7
sudo apt install python2.7
```

```sh
# Instalar pip para gestionar las librerías Python.
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
```

Si prefieres instalar todas las librerias de Python de un plumazo puedes utilizar el fichero requeriments.txt que se encuentra en la raiz del proyecto y ejecutar.
```sh
pip install -r requirements.txt
```
Ahora ya están todas las librerías de Python instaladas no obstante se da una alternativa manual para instalarlas en los pasos 
posteriores. Omite los que empiecen con sudo pip install.

Nota: Existen soluciones mejores que pip que permiten la virtualización de la gestión de paquetes y permitir la mejor convivencia de diversos proyectos python. Hablamos de pipenv.


###Instalar cliente para MySQL

```sh
# Instalar dependencias para librería de python mysqlclient
sudo apt install python-dev libmysqlclient-dev
sudo pip install mysqlclient
```

### Instalar pandas
```sh
# Instalar pandas para la lectura y transformación de datos
sudo pip install pandas

```
### Instalar MongoDB y un cliente Python

```sh
# Instalar conector para MongoDB
sudo pip install pymongo

```

```sh
#Importar una clave publica para poder descargar el paquete mongo db
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4
```
```sh
# Crear una lista de ficheros para mongo db
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/4.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.0.list
```
```sh
# Instalar el paquete
sudo apt-get update
sudo apt-get install -y mongodb-org
```
```sh
# Arrancar el servicio
sudo service mongod start
```
```sh
# Comprobar que el servidor acepta peticiones (debes ver waiting for connections on port 27017)
cat /var/log/mongodb/mongod.log | grep waiting
```
```sh
#Arrancar el servidor
mongo --host 127.0.0.1:27017
```

## Descargar el repositorio de código de GitHub
```sh
sudo mkdir /opt/repos
chown ubuntu:ubuntu repos/
cd /opt/repos/
git clone https://github.com/nordev/plan-test.git
```

## Fijar PYTHONPATH

```sh
# La variable de entorno PythonPath con el valor de la raiz del repo.
export PYTHONPATH=/opt/repos/plan-test/
```

## Comprobar que todo funciona
```sh
# Ejecuta los test para la clase Extractor
python -m unittest test_extractor.TestExtractor
```
```sh
# Ejecuta los test para toda la aplicación
sh /opt/repos/plan-test/test/execute_test.sh
```


# Planificación de la ejecución

# Ejecución de la aplicación
```sh
python /opt/repos/plan-test/src/controller.py
```

# Sobre el diseño de la aplicación

Aunque se podría haber programado la aplicación entera en un solo script.
Se ha buscado poner en valor otras skills. Se utiliza la programación orientada a objetos para descomponer y encapsular 
la aplicación en varias clases siguiendo la lógica de un proceso ETL.
Se hacen test unitarios que permiten probar aisladamente la funcionalidad de diversas clases u hacer refactoring de las mismas.
Se utiliza el patrón de diseño MVC (Modelo Vista Controlador).
Aunque no exista la vista dejamos la arquitectura preparada.
En general a pesar del breve tiempo de desarrollo se han intendo seguir las buenas prácticas de programación y código limpio.

# Temas de Eficiencia y Críticas a la aplicación

Como se puede observar en el repositorio de código se han creado más funciones de las que finalmente son utilizadas por el controlador.
Estas pretenden ilustrar diferentes escenarios en función de los requerimentos de hardware que pudieramos tener.
Se han programado funciones que permiten escribir en ficheros los datos resultantes del proceso de extracción. También es posible escribir en ficheros los datos resultantes del proceso de transformación.
Esta funcionalidad se usa para los test porque permite aislar los diferentes procesos. En cambio para el controlador se ha tomado la decisión de cargar todo en memoria. Para una situación real sería 
probablemente inviable. Aunque parte para este programa de juguete representa una buena solución. En una situación con un gran volumen de datos, una buena solución sería cargar en memoria la información
por bloques de un tamaño razonable dependiendo de nuestro Hardware.
O si nuestros recursos son más limitados trabajar por bloques y con ficheros como "buffers" intermedios.


## Extracción
El proceso de extracción de datos a la Base de Datos se hace a través de una librería Python llamada MySQLdb.
Se ha tomado una alternativa altamente ineficiente que es hacer un select de la tabla y escribir el 
cursor en un fichero o cargarlo en memoria en forma de DataFrame. Probablemente existan otras formas más
eficientes de hacer este proceso como hacer dump de la base de datos o escribirla en binario.

## Transformación
Se utiliza la librería de Python llamada Pandas para realizar todas las transformaciones. Por poner otro punto negativo 
a la eficiencia. Se ha creado en las dos "tablas" una columna llamada "_id" que se encarga de crear un id único para el 
posterior alamacenamiento en BD. Este id es una concatenación de strings que resulta ser bastante largo
y repercute en la cantidad de almacenamiento en disco necesario en BD. Tampoco he entrado demasiado en temas de data cleaning, más alla de borrar registros duplicados y cuestiones triviales.
Por poner algún ejemplo he detectado que el sistema operativo IOS aparece en algunos registros como "IOS" y en otros como "ios". Se podrían haber hecho más tareas de limpieza como trimming o lowercase. 


## Carga
La primera carga se hace de una forma muy eficiente. Se suben todos los datos que están cargados en memoria en batch.
en batch a la Base de Datos. En las siguientes ejecuciones la eficicencia de este proceso depende de la naturaleza de los datos 
que se suban. Es decir, si en una segunda subida se detecta que hay alguna clave duplicada. El proceso de inserción/actualización
ya no se realiza en batch. Si no que se realiza linea a linea. Una mejor aproximación sería detectar todas los registros
que van a ser actualizados y hacer una actualización en batch. Esto no se ha implementado por falta de tiempo.


