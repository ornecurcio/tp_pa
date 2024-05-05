import json
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

# Conexión a la base de datos
conn = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"), 
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# 1. Cantidad de advertisers
cursor.execute("SELECT COUNT(DISTINCT advertiser_id) FROM ctr")
num_advertisers = cursor.fetchone()[0]

# 2. Advertisers que más varían sus recomendaciones por día
cursor.execute("""
    SELECT advertiser_id, COUNT(DISTINCT date) AS days_count
    FROM ctr
    GROUP BY advertiser_id
    ORDER BY days_count DESC
""")
advertisers_variability = cursor.fetchall()

# 3. Estadísticas de coincidencia entre product_id para los diferentes advertisers
cursor.execute("""
    SELECT product_id, COUNT(DISTINCT advertiser_id) AS advertisers_count
    FROM ctr
    GROUP BY product_id
    HAVING COUNT(DISTINCT advertiser_id) > 1
    ORDER BY advertisers_count DESC
""")
product_overlap_stats = cursor.fetchall()

# Cerrar cursor y conexión
cursor.close()
conn.close()

# Crear el archivo JSON con las estadísticas
stats = {
    "num_advertisers": num_advertisers,
    "advertisers_variability": advertisers_variability,
    "product_overlap_stats": product_overlap_stats
}

with open("statistics.json", "w") as outfile:
    json.dump(stats, outfile, indent=4)
