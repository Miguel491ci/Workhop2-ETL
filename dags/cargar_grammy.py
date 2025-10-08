import pandas as pd
from sqlalchemy import create_engine

# 1️⃣ Cargar el CSV en un DataFrame
df = pd.read_csv(r"C:\Users\miguel\OneDrive\Escritorio\Airflow Docker\the_grammy_awards.csv")

# 2️⃣ Limpieza y transformación de datos
df["winner"] = df["winner"].map({"True": 1, "False": 0})
df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

# 3️⃣ Configurar la conexión a MySQL
usuario = "root"
contraseña = ""     # sin contraseña
host = "localhost"
puerto = "3306"
base_datos = "workshop2"

# Crear el motor de conexión
engine = create_engine(f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}")

# 4️⃣ Exportar el DataFrame a MySQL
df.to_sql(
    name="grammy",       # nombre de la tabla en MySQL
    con=engine,
    if_exists="replace", # reemplaza la tabla si ya existe (usa "append" si solo quieres añadir)
    index=False
)

print("✅ Datos importados exitosamente a la tabla 'grammy'")
