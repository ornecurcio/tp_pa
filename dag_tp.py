import datetime
import numpy as np
import pandas as pd
import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='filtrar_datos',
    schedule='0 0 * * *',
    start_date=datetime.datetime(2024, 4, 1),
    catchup=False,
) as dag:

    def join_csv(date,file1, file2, output_file):
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        df_joined = pd.merge(df1, df2, on='advertiser_id')
         # Filtrar por la fecha especificada
        df_joined = df_joined[df_joined['date'] == date]
        df_joined.to_csv(output_file, index=False)
    
    def top_product(file, output_file):
        # Leer el archivo CSV
        df = pd.read_csv(file)
        # Agrupar por advertiser_id y product_id y contar las ocurrencias
        grouped = df.groupby('advertiser_id')
        # Crear un DataFrame vacío para almacenar los resultados
        top_20_products_per_advertiser = pd.DataFrame()
        for name, group in grouped:
            top_20_products = group['product_id'].value_counts().head(20)
            top_20_products = top_20_products.reset_index()
            top_20_products.columns = ['product_id', 'count']
            top_20_products['advertiser_id'] = name
            top_20_products_per_advertiser = pd.concat([top_20_products_per_advertiser, top_20_products])
        # Reorganizar las columnas
        top_20_products_per_advertiser = top_20_products_per_advertiser[['advertiser_id', 'product_id', 'count']]
        # Guardar el resultado en un archivo CSV
        top_20_products_per_advertiser.to_csv(output_file, index=False)

    def top_ctr(file,output_file):
        df = pd.read_csv(file)
        # Crear columnas separadas para click e impression
        df['click'] = df['type'].apply(lambda x: 1 if x == 'click' else 0)
        df['impression'] = df['type'].apply(lambda x: 1 if x == 'impression' else 0)
        # Agrupar por advertiser_id y product_id y calcular el total de click e impression
        grouped = df.groupby(['advertiser_id', 'product_id']).agg({'click': 'sum', 'impression': 'sum'}).reset_index()
        # Calcular el CTR
        grouped['ctr'] = grouped['click'] / grouped['impression']
        # Ordenar por CTR y obtener el top 20
        top_20_ctr = grouped.sort_values('ctr', ascending=False).head(20)
        # Guardar el resultado en un archivo CSV
        top_20_ctr.to_csv(output_file, index=False)

    filter_data_product = PythonOperator(
        task_id='product_active_csv',
        python_callable=join_csv,
        op_kwargs={'date': f"{datetime.datetime.now().strftime('%Y-%m-%d')}",
                   'file1': '/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/advertiser_ids.csv', 
                   'file2': '/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/product_views.csv', 
                   'output_file': f"/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/active/product_active_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv" 
                   }
    )
    filter_data_ads = PythonOperator(
        task_id='ads_active_csv',
        python_callable=join_csv,
        op_kwargs={'date': f"{datetime.datetime.now().strftime('%Y-%m-%d')}",
                   'file1': '/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/advertiser_ids.csv', 
                   'file2': '/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/ads_views.csv', 
                   'output_file': f"/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/active/ads_active_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
                   }
    )
    top_product_task = PythonOperator(
        task_id='top_product',
        python_callable=top_product,
        op_kwargs={'file': f"/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/active/product_active_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv",
                   'output_file': f"/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/active/top_product_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
                   }
    )
    top_ctr_task = PythonOperator(
        task_id='top_ctr',
        python_callable=top_ctr,
        op_kwargs={'file': f"/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/active/ads_active_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv",
                   'output_file': f"/Users/ocurcio/Documents/MasterDataScience/ProgramacionAvanzada/TP/files/active/top_ctr_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
                   }
    )
    # Podemos agrupar varias tareas al definir precedencias
    # [filter_data_product, filter_data_ads] >> top_product_task
    filter_data_product >> top_product_task
    filter_data_ads >> top_ctr_task