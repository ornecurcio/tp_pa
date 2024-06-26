from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

engine = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"), 
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = engine.cursor()
# cursor.execute("""
#                CREATE TABLE IF NOT EXISTS product ( 
#                     date DATE,
#                     advertiser_id VARCHAR(255),
#                     product_id VARCHAR(255),
#                     count INT,
#                     PRIMARY KEY (date, advertiser_id, product_id)
#                     );
#                """)
# cursor.execute("""
#                CREATE TABLE IF NOT EXISTS ctr ( 
#                     date DATE,
#                     advertiser_id VARCHAR(255),
#                     product_id VARCHAR(255),
#                     impressions FLOAT,
#                     clicks FLOAT,
#                     CTR FLOAT,
#                     PRIMARY KEY (date, advertiser_id, product_id)
#                     );
#                """)
# Extraer la fecha del nombre del archivo
# file_name = 'files/top_ctr_test_2024-04-30.csv'
# date = os.path.splitext(file_name)[0].split('_')[-1]  # '2024-04-30'
# print(date)

#Leer el archivo CSV

# df1 = pd.read_csv(file_name)
# insert_query = 'INSERT INTO ctr (date, advertiser_id, product_id, impressions, clicks, CTR) VALUES (%s, %s, %s, %s, %s, %s)'
# for index, row in df1.iterrows():
#     cursor.execute(insert_query, (date, row['advertiser_id'], row['product_id'], row['impressions'], row['clicks'], row['CTR']))



#cursor.execute("""SELECT * FROM ctr WHERE date = TO_DATE(%s, 'YYYY-MM-DD');""", ('2024-05-02',)) 

# cursor.execute("""DELETE FROM ctr;""")
# cursor.execute('SELECT DISTINCT date FROM ctr;')
# cursor.execute("""SELECT Distinct advertiser_id FROM ctr WHERE date = TO_DATE(%s, 'YYYY-MM-DD');""", ('2024-05-03',))
cursor.execute("""
    SELECT * FROM ctr 
    WHERE date = TO_DATE(%s, 'YYYY-MM-DD')""", ('2024-05-13',))

#SELECT advertiser_id, COUNT(*)
#GROUP BY advertiser_id


# Obtener los resultados de la consulta
# results = cursor.fetchall()

# Convertir los resultados en un DataFrame
# df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

# df.to_csv("test.csv")

#WHERE date = TO_DATE(%s, 'YYYY-MM-DD') groupby advertiser_id""", ('2024-05-03',))
# (LXH5XTBJTBCH8IRH3B0)
rows = cursor.fetchall()
print('Primera query')
print(rows)
# cursor.execute("""SELECT * FROM ctr WHERE date = TO_DATE(%s, 'YYYY-MM-DD');""", ('2024-05-01',)) 
# rows = cursor.fetchall()
# print('Segunda query')
# print(rows)
engine.commit()
cursor.close()
engine.close()

# cursor.execute("""INSERT INTO usuarios (nombre) VALUES ('matias')";"") cursor.execute("""SELECT * FROM usuarios;""")
# cursor.execute("""SELECT product_id FROM product;""") 
# rows = cursor.fetchall()
# print('Segunda query')
# print(rows) 
# engine.commit()