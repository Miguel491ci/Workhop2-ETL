from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import re
import mysql.connector
import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from sqlalchemy import create_engine
import mysql.connector

# Ruta del CSV
SPOTIFY_CSV_PATH = "/opt/airflow/data/spotify_dataset.csv"

# Configuración conexión MySQL
DB_CONFIG = {
    'host': 'host.docker.internal',  # permite que Airflow (en Docker) acceda al localhost real
    'user': 'root',
    'password': '',
    'database': 'workshop2',
    'port': 3306
}

@dag(
    dag_id="etl_spotify_grammy",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual run
    catchup=False,
    tags=["ETL", "Spotify", "Grammys"]
)
def etl_spotify_grammy():

    @task()
    def extract_spotify():
        """Extrae el dataset de Spotify desde un CSV."""
        if not os.path.exists(SPOTIFY_CSV_PATH):
            raise FileNotFoundError(f"No se encontró el archivo: {SPOTIFY_CSV_PATH}")
        
        df_spotify = pd.read_csv(SPOTIFY_CSV_PATH)
        logging.info(f"✅ Spotify dataset cargado correctamente.")
        logging.info(f"Filas: {len(df_spotify)}, Columnas: {len(df_spotify.columns)}")
        logging.info(f"Columnas: {list(df_spotify.columns)}")
        logging.info(f"Primeras filas:\n{df_spotify.head(3).to_string()}")

        csv_path = "/opt/airflow/data/spotify_temp.csv"
        df_spotify.to_csv(csv_path, index=False)  # guardar CSV
        return csv_path  # devolver ruta en lugar de diccionario

    @task()
    def extract_grammy():
        """Extrae la tabla Grammy desde la base de datos MySQL."""
        conn = mysql.connector.connect(**DB_CONFIG)
        df_grammy = pd.read_sql("SELECT * FROM grammy", conn)
        conn.close()

        logging.info(f"✅ Grammy dataset cargado correctamente.")
        logging.info(f"Filas: {len(df_grammy)}, Columnas: {len(df_grammy.columns)}")
        logging.info(f"Columnas: {list(df_grammy.columns)}")
        logging.info(f"Primeras filas:\n{df_grammy.head(3).to_string()}")

        csv_path = "/opt/airflow/data/grammy_temp.csv"
        df_grammy.to_csv(csv_path, index=False)
        return csv_path

    # Definimos los outputs
    spotify_data = extract_spotify()
    grammy_data = extract_grammy()



    @task()
    def transform_data(spotify_csv_path: str, grammy_csv_path: str):
        """Limpieza, imputación y combinación de datasets Spotify + Grammy."""
        
        # Leer los archivos
        df_spotify = pd.read_csv(spotify_csv_path)
        df_grammy  = pd.read_csv(grammy_csv_path)


        # ==============================
        # 🔹 LIMPIEZA INICIAL GRAMMY
        # ==============================
        df_grammy = df_grammy[~((df_grammy['nominee'] == '') & (df_grammy['artist'] == ''))]

        columnas_a_eliminar = ['published_at', 'updated_at', 'workers', 'img']
        df_grammy = df_grammy.drop(columns=columnas_a_eliminar, errors='ignore')

        # ==============================
        # 🔹 IMPUTACIÓN DE ARTISTAS FALTANTES
        # ==============================
        grammy = df_grammy.copy()
        spotify = df_spotify.copy()

        grammy['category_norm'] = grammy['category'].fillna('').astype(str).str.lower().str.strip()
        grammy['nominee_norm']  = grammy['nominee'].fillna('').astype(str).str.lower().str.strip()
        grammy['artist_norm']   = grammy['artist'].fillna('').astype(str).str.lower().str.strip()

        spotify['album_name_norm'] = spotify['album_name'].fillna('').astype(str).str.lower().str.strip()
        spotify['artists_norm']    = spotify['artists'].fillna('').astype(str).str.lower().str.strip()

        # Coincidencias exactas por álbum
        mask_album = grammy['category_norm'].str.contains('album', na=False)
        grammy_album = grammy[mask_album].copy()
        mask_no_artist = grammy_album['artist_norm'] == ''
        grammy_album_no_artist = grammy_album[mask_no_artist].copy()
        grammy_album_no_artist['original_index'] = grammy_album_no_artist.index

        merge_album = grammy_album_no_artist.merge(
            spotify[['album_name_norm', 'artists_norm']],
            left_on='nominee_norm',
            right_on='album_name_norm',
            how='left'
        )

        matched = merge_album.loc[merge_album['artists_norm'].notna()]
        for _, row in matched.iterrows():
            df_grammy.at[row['original_index'], 'artist'] = row['artists_norm']

        # Reemplazar vacíos restantes por "Unknown"
        df_grammy['artist'] = df_grammy['artist'].replace('', pd.NA).fillna('Unknown')

        # ==============================
        # 🔹 LIMPIEZA SPOTIFY
        # ==============================
        df_spotify = df_spotify.drop_duplicates(keep='first')

        # ==============================
        # 🔹 NORMALIZACIÓN Y UNIÓN FINAL
        # ==============================
        grammy = df_grammy.copy()
        spotify = df_spotify.copy()

        # Función de normalización
        def normalize_text(s):
            if pd.isna(s):
                return ''
            s = s.lower().strip()
            s = re.sub(r'\s*(feat\.|featuring|ft\.|&|and)\s*', ';', s)
            s = re.sub(r'\s+', ' ', s)
            return s

        for col in ['category', 'nominee', 'artist']:
            grammy[f'{col}_norm'] = grammy[col].astype(str).apply(normalize_text)

        spotify['track_name_norm'] = spotify['track_name'].astype(str).str.lower().str.strip()
        spotify['album_name_norm'] = spotify['album_name'].astype(str).str.lower().str.strip()
        spotify['artists_orig'] = spotify['artists']
        spotify['artists_norm'] = spotify['artists'].astype(str).apply(
            lambda x: x.lower().replace('&', ';').replace(',', ';')
        )

        # Clasificación por tipo
        song_keywords = ['song', 'performance', 'recording', 'music', 'composition', 'track']
        mask_song = grammy['category_norm'].apply(lambda x: any(k in x for k in song_keywords))

        grammy_song = grammy[mask_song].copy()
        grammy_other = grammy[~mask_song].copy()

        # Mantener solo la versión más popular
        spotify_top = (
            spotify.sort_values('popularity', ascending=False)
            .drop_duplicates(subset=['artists_norm', 'track_name_norm'])
        )

        # Merge canción-artista flexible
        merged_song = []

        for _, row in grammy_song.iterrows():
            artist = row['artist_norm']
            song = row['nominee_norm']

            # Coincidencia exacta
            match = spotify_top[
                (spotify_top['artists_norm'].str.contains(artist, na=False)) &
                (spotify_top['track_name_norm'] == song)
            ]

            # Si no hay match exacto → coincidencia parcial
            if match.empty:
                safe_song = re.escape(song.split('(')[0].strip())
                match = spotify_top[
                    (spotify_top['artists_norm'].str.contains(artist, na=False)) &
                    (spotify_top['track_name_norm'].str.contains(safe_song, na=False, regex=True))
                ]

            if not match.empty:
                best = match.sort_values('popularity', ascending=False).iloc[0]
                combined = pd.concat([row, best])
                merged_song.append(combined)
            else:
                merged_song.append(row)

        merged_song = pd.DataFrame(merged_song)

        # Combinar con otras categorías
        merged_total = pd.concat([merged_song, grammy_other], ignore_index=True)

        # Resumen
        print("🎧 Ejemplo unión por canción:")
        print(merged_song[['category', 'nominee', 'artist', 'track_name', 'album_name', 'popularity']].head(5))
        print(f"\n📊 Tamaños → Canción: {len(merged_song)}, Otros: {len(grammy_other)}")
        print(f"➡️ Total final combinado: {len(merged_total)} registros")

        # Limpieza final
        cols_to_drop = [
            'id', 'category_norm', 'nominee_norm', 'artist_norm',
            'Unnamed: 0', 'track_name_norm', 'album_name_norm',
            'artists_orig', 'artists_norm'
        ]

        merged_clean = merged_total.drop(columns=[c for c in cols_to_drop if c in merged_total.columns], errors='ignore')
        merged_clean = merged_clean.fillna("N/A")

        # Convertir valores de texto a booleanos
        merged_clean["winner"] = merged_clean["winner"].astype(str).str.lower().map({"true": True, "false": False})
        merged_clean["winner"] = merged_clean["winner"].astype(bool)

        print(f"✅ Dataset limpio guardado con {len(merged_clean)} registros y {len(merged_clean.columns)} columnas.")

        # Retornar para la siguiente task
        return {
            "merged": merged_clean.to_dict()
        }

    transformed_data = transform_data(
    spotify_csv_path=spotify_data,  # spotify_data ahora es la ruta CSV
    grammy_csv_path=grammy_data     # grammy_data ahora es la ruta CSV
    )

    @task()
    def load_data(transformed_data: dict):
        """Carga el dataset final a Google Drive y MySQL."""


        # ==============================
        # 🔹 CONFIGURACIONES
        # ==============================
        OUTPUT_CSV_PATH = "/opt/airflow/dags/merged_final_grammy_spotify_clean.csv"
        FOLDER_ID = "1k2hWAVczuDBV2ajN8d5ST6Y84w3lfDXi"  # <- tu carpeta de Drive

        DB_USER = "root"
        DB_PASSWORD = ""
        DB_HOST = "host.docker.internal"  # permite conexión al host desde Docker
        DB_PORT = "3306"
        DB_NAME = "workshop2"

        # ==============================
        # 🔹 CONVERTIR Y GUARDAR CSV
        # ==============================
        merged_df = pd.DataFrame(transformed_data["merged"])
        merged_df.to_csv(OUTPUT_CSV_PATH, index=False)
        print(f"💾 CSV final guardado en {OUTPUT_CSV_PATH}")

        # ==============================
        # 🔹 CARGA A GOOGLE DRIVE
        # ==============================
        SCOPES = ['https://www.googleapis.com/auth/drive.file']

        if not os.path.exists("/opt/airflow/dags/credentials.json"):
            raise FileNotFoundError("⚠️ Falta el archivo credentials.json en /opt/airflow/dags")

        if not os.path.exists("/opt/airflow/dags/token.json"):
            flow = InstalledAppFlow.from_client_secrets_file("/opt/airflow/dags/credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
            with open("/opt/airflow/dags/token.json", "w") as token:
                token.write(creds.to_json())
        else:
            creds = Credentials.from_authorized_user_file("/opt/airflow/dags/token.json", SCOPES)

        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': 'merged_final_grammy_spotify_clean.csv',
            'parents': [FOLDER_ID]
        }
        media = MediaFileUpload(OUTPUT_CSV_PATH, mimetype='text/csv')

        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

        print(f"✅ Archivo subido a Google Drive con ID: {uploaded_file.get('id')}")

        # ==============================
        # 🔹 CARGA A MYSQL
        # ==============================
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME};")
        cursor.close()
        conn.close()

        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        merged_df.to_sql("clean_grammy_spotify", con=engine, if_exists="replace", index=False)

        print("✅ Carga completada. Tabla 'clean_grammy_spotify' creada exitosamente en MySQL.")

    loaded_data = load_data(transformed_data)

etl_spotify_grammy()
