## Workshop 2: ETL y Análisis Grammy + Spotify con Airflow

# 📂 Estructura del proyecto
Airflow Docker/
│

├── dags/                     # DAGs de Airflow

|  ├── cargar_grammy.py

|  └── etl_grammy_spotify.py

├── data/                     # Datasets originales y finales

│   ├── spotify_dataset.csv

│   └── merged_final_grammy_spotify_clean.csv

├── -gitignore

├── README.md

├── docker-compose.yaml       # Configuración de Airflow en Docker

├── eda.ipynb                 # Notebook de análisis exploratorio

└── requirements.txt          # Dependencias del proyecto


# 🔧 Requisitos

Docker y Docker Compose instalados

Python 3.9+

Librerías del requirements.txt (pandas, re, etc.)

Acceso a los datasets de Spotify y Grammy (CSV)


# 🚀 Cómo ejecutar

Clonar el repositorio:

git clone https://github.com/Miguel491ci/Workhop2-ETL.git
cd Airflow\ Docker

Construir y levantar Airflow con Docker Compose:

docker-compose up -d

Acceder al UI de Airflow:

URL por defecto: http://localhost:8080

Usuario/Contraseña: admin/admin (o según tu configuración)

Ejecutar el DAG spotify_grammy_etl desde la interfaz de Airflow.

Al terminar, los datos combinados se encontrarán en:

data/merged_final_grammy_spotify_clean.csv


# 🧹 Explicaciones del ETL Pipeline

**Extracción**
Se empieza extrayendo ambos datasets de sus fuentes originales. Por un lado el dataset de spotify se extrae directamente del archivo spotify_dataset.csv cargado en la carpeta data del repositorio. Por otro lado, para extraer el dataset de grammys (que se asume previamente cargado en la base de datos) se debe realizar la conexion a la base de datos en el que se encuentra para posteriormente transformar esa tabla con su contenido en un dataframe de pandas.

**Transformacion**
En la transformación se realizaron dos procesos principales de suma importancia: La limpieza de ambos datasets, y la union de los mismos con el fin de ampliar la información en el Dataset de grammys.
para empezar, en la limpieza se realizaron las siguientes acciones:

  - Limpieza de datos vacíos o inconsistentes: Entendiendo que no podemos trabajar con datos vacios ya que pueden generar problemas para futuras tareas, se  estandarizaron los vacios de todas las columnas **excepto artist** con N/A para ambos datasets (aunque el de spotify no presentaba nulos).
  - Normalización de nombres de artistas, canciones y categorías: Esta etapa es clave teniendo en cuenta el objetivo final de la transformacion que es la union de ambos datasets, pues si no normalizamos a la hora de unir informacion que es igual pero esta registrada con ligeros cambios entre ellos se va a perder o no se podra unir de manera satisfactoria.
  - Imputación de artistas por álbum o canción: Esta de aqui es la parte mas importante de la limpieza dentro del proceso de transformacion. Como se decidio posteriormente unir ambos datasets mediante la columna artista que es la mas similar que tienen entre ellos, tener la mayor cantidad de registros de artista es clave para evitar perder la mayor informacion posible, por ende se realizaron 3 metodos de imputacion dependiendo de la categoria del grammy:
      - Imputacion por nombre de artista en la columna nominee: En algunos casos, en los premios que se le entregan al artista en si, se guardaba el nombre del artista en la columna nominado pero no se replicaba en la columna artista, asi que se copio y pego esa informacion
      - Imputacion por nombre de album en la columna nominee: Para los premios que se entregaban por album, se tomo el valor de la columna nominee que precisamente guardaba los nombres de los albums nominados, y con esta informacion se comparo dentro del dataset de spotify para comprobar si habia algun registro que coincidiera en su columna de album_name con dicho nombre, en caso de que coincidan, se tomaria la informacion de la columna artist del dataset de spotify y se pegaria esa informacion en la columna artist del registro que se estaba analizando del dataset de grammy
      - Imputacion por nombre de cancion en la columna nominee: Similar al caso de nombre de album, pero entonces comparamos los nombres de la cancion de la columna nominee del dataset de grammy con el track_name del dataset de spotify

Ahora bien, para la union de datasets se aprovecho la tecnica de filtrado que se hizo durante la limpieza, y se procedio a hacer un merge entre ambos datasets mediante la columna artista y nominee. Cuando el premio se entregaba a artista, se unieron los nombres de los artistas de ambos datasets. Cuando el premio se entregaba al album, se unio el album_name de spotify con el nominee de esas categorias que precisamente eran nombres de albums. Finalmente cuando el premio se entregaba a canciones, se junto el track_name del dataset de spotify con el valor de la columna nominee que precisamente era el nombre de la cancion en cuestion. Esta ultima union es la mas importante debido a que como estamos uniendole un dataset de canciones a ese dataset de grammys, estos son los registros que van a ver su informacion extendida con los valores importantes del dataset de Spotify. Con los datasets unidos, finalmente se hizo una limpieza pequeña final.

**Carga**
Finalmente para la carga se realizó la conexion con el data warehouse en mysql workbench, donde se cargó este dataset resultante de la union de los dos originales. Tambien se realizo la carga del dataset convertido a csv a una carpeta de google drive mediante la API de google drive.

## Airflow DAG design

El orquestador de este proyecto fue AirFlow mediante contenedores de Docker. Se utilizo precisamente para dejar toda la logica del pipeline de ETL estructurada y dividida por tareas o tasks, precisamente este proceso se dividio en 4 tasks compuestas dentro del dag que contiene el archivo etl_grammy_spotify.py:

- extract_spotify: Cumple la tarea de extraer el csv de spotify y su informacion
- extract_grammy: Realiza la conexion con la base de datos y trae la informacion de la tabla de grammy
- transform_data: Realiza todo el proceso de transformacion explicado previamente
- load_data: Carga el dataset final resultante de la union de los dos originales al data warehouse y a google drive

A continuacion se presenta una imagen de la estructura para evidenciar que las tareas se hacen en el orden debido:
<img width="691" height="317" alt="image" src="https://github.com/user-attachments/assets/8eebda65-d365-4068-b780-d15711f38318" />


# 📈 KPIs destacados

Como la idea pensada para este negocio es mostrarle insights a una disquera que tenga como objetivo ganar un grammy, se especificaron 4 KPIs que pueden facilitar este proceso analizando distintas variables de los premios y sus detalles de sus canciones ganadoras:

Artista más premiado: Total de Grammys por artista. (Para tener un referente)

Nivel de Popularidad: Nivel de popularidad de canciones ganadoras del premio Song Of The Year. (Para ver no solo cuales han ganado el reconocimiento mas importante, sino ver que tanto la popularidad puede llegar a afectar en la decision)

Influencia de la explicitez de la cancion al ganar premio. (Para saber si esto influye y si se debe tener en cuenta)

Cantidad de premios por genero musical. (Con esto se decide con que genero se tienen mas probabilidades de ganar un grammy)


# 📊 Análisis exploratorio y visualizaciones

Se realizaron varias gráficas para explorar tendencias y relaciones entre los datasets, y graficar los KPIs establecidos previamente:

- Recuento de ganadores de Grammy por artista

- Nivel de popularidad de las canciones ganadoras del Song of the Year

- Danzabilidad de canciones ganadoras del Song of the Year

- Pie chart de cantidad de Grammys por género

- Cantidad de Grammys según si la canción es explícita o no


# ⚠️ Notas

Todos los archivos de credenciales (token.json, credentials.json) han sido excluidos del repositorio por motivos de seguridad.

Los datasets CSV son necesarios para que el DAG funcione correctamente.
