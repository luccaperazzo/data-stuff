import requests
import pandas as pd
import sqlite3
from datetime import datetime

# 1. EXTRAER DATOS (Extract)
def extract_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al extraer datos. Código: {response.status_code}")

# 2. TRANSFORMAR DATOS (Transform)
def transform_data(data):
    transformed_data = {
        'fact': data['fact'],
        'length': data['length'],
        'timestamp': datetime.now()
    }
    df = pd.DataFrame([transformed_data])  
    return df

# 3. CARGAR DATOS (Load)
def load_data_to_db(df, db_name):
    # Conexión a SQLite
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Crear tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cat_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fact TEXT NOT NULL,
            length INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insertar datos
    df.to_sql('cat_facts', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()

# Configuración del pipeline
if __name__ == "__main__":
    # URL de la API de Cat Facts
    API_URL = "https://catfact.ninja/fact"
    
    # Nombre de la base de datos
    DB_NAME = 'C:\\Users\\Usuario\\Desktop\\sqlite-tools-win-x64-3470200\\cat_facts.db'


    
    # Pipeline ETL
    try:
        print("Extrayendo datos...")
        raw_data = extract_data(API_URL)
        
        print("Transformando datos...")
        transformed_data = transform_data(raw_data)
        
        print("Cargando datos en la base de datos...")
        load_data_to_db(transformed_data, DB_NAME)
        
        print("¡Pipeline ETL completado!")
    except Exception as e:
        print(f"Error en el pipeline: {e}")
