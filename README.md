Workshop 2: ETL y Análisis Grammy + Spotify con Airflow
📂 Estructura del proyecto
Airflow Docker/
│
├── dags/                     # DAGs de Airflow
├── data/                     # Datasets originales y finales
│   ├── spotify_dataset.csv
│   └── merged_final_grammy_spotify_clean.csv
├── docker-compose.yaml       # Configuración de Airflow en Docker
├── requirements.txt          # Dependencias del proyecto
├── eda.ipynb                 # Notebook de análisis exploratorio
├── logs/                     # Logs generados por Airflow
└── README.md

🔧 Requisitos

Docker y Docker Compose instalados

Python 3.9+

Librerías del requirements.txt (pandas, re, etc.)

Acceso a los datasets de Spotify y Grammy (CSV)

🚀 Cómo ejecutar

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

🧹 Transformaciones realizadas

Limpieza de datos vacíos o inconsistentes.

Normalización de nombres de artistas, canciones y categorías.

Imputación de artistas por álbum o canción.

Merge flexible entre Grammy y Spotify (canción/álbum/artista).

Conversión de columna winner a boolean.

Eliminación de columnas auxiliares.

📊 Análisis exploratorio y visualizaciones

Se realizaron varias gráficas para explorar tendencias y relaciones entre los datasets:

Recuento de ganadores de Grammy por artista

Nivel de popularidad de las canciones ganadoras del Song of the Year

Danzabilidad de canciones ganadoras del Song of the Year

Pie chart de cantidad de Grammys por género

Cantidad de Grammys según si la canción es explícita o no

📈 KPIs destacados

Artista más premiado: Total de Grammys por artista.

Popularidad promedio: Nivel de popularidad de canciones ganadoras.

Danzabilidad promedio: Promedio de danceability de canciones ganadoras.

⚠️ Notas

Todos los archivos de credenciales (token.json, credentials.json) han sido excluidos del repositorio por motivos de seguridad.

Los datasets CSV son necesarios para que el DAG funcione correctamente.
