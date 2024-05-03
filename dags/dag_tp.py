import datetime
import numpy as np
import pandas as pd
import boto3
import os
import psycopg2
from dotenv import load_dotenv 
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='filtrar_datos',
    schedule='0 7 * * *',
    start_date=datetime.datetime(2024, 4, 1),
    catchup=False,
) as dag:

    load_dotenv()
    s3 = boto3.client(
            's3',
            region_name='us-east-2',
            aws_access_key_id= os.getenv("ACCESS_KEY"),
            aws_secret_access_key= os.getenv("SECRET_KEY")
        )

    def connect_db():
        return psycopg2.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"), 
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

    def join_csv(date, file1, file2, **context):
        df1 = s3.get_object(Bucket = os.getenv("BUCKET_NAME"), Key = file1)
        df2 = s3.get_object(Bucket = os.getenv("BUCKET_NAME"), Key = file2)
        # Leer los archivos CSV
        df1 = pd.read_csv(df1['Body'])
        df2 = pd.read_csv(df2['Body'])
        # Unir los DataFrames
        df = pd.merge(df1, df2, on='advertiser_id')
        # Filtrar por fecha
        df = df[df['date'] == date]
        print(df.head())
        # Empujar el DataFrame en XComs
        context['task_instance'].xcom_push('data', df.to_json())

    def top_product(**context):
        # Traer el DataFrame de XComs
        df_json = context['task_instance'].xcom_pull(task_ids='product_active', key='data')
        df = pd.read_json(df_json)
        # Calcular el top 20 de productos por cada advertiser_id
        def get_top_20_products(group):
            return group.sort_values('count', ascending=False).head(20)

        result = df.groupby(['date', 'advertiser_id', 'product_id']).size().reset_index(name='count')
        top_20_per_advertiser = result.groupby('advertiser_id').apply(get_top_20_products)

        # Debido a la naturaleza de 'apply', los índices podrían necesitar un ajuste.
        top_20_per_advertiser = top_20_per_advertiser.reset_index(drop=True)
        print(top_20_per_advertiser)
        context['task_instance'].xcom_push('data', top_20_per_advertiser.to_json())

    def write_product_to_db(**context):
        df_json = context['task_instance'].xcom_pull(task_ids='top_product', key='data')
        df = pd.read_json(df_json)
        with connect_db() as engine:
            with engine.cursor() as cursor:
                insert_query = 'INSERT INTO product (date, advertiser_id, product_id, count) VALUES (%s, %s, %s, %s);'
                for _, row in df.iterrows():
                    cursor.execute(insert_query, (row['date'], row['advertiser_id'], row['product_id'], row['count']))
            engine.commit()

        

    filter_data_product = PythonOperator(
    task_id='product_active',
    python_callable=join_csv,
    op_kwargs={'date': f"{datetime.datetime.now().strftime('%Y-%m-%d')}", 
            'file1' : 'advertiser_ids.csv', 
            'file2' : 'product_views.csv'},
    provide_context=True  # Necesario para acceder a task_instance
    )

    top_product_task = PythonOperator(
        task_id='top_product',
        python_callable=top_product,
        provide_context=True,  # Necesario para acceder a task_instance
    )

    top_product_write_db = PythonOperator(
        task_id='product_write_db',
        python_callable=write_product_to_db,
        provide_context=True
    )
    filter_data_product >> top_product_task >> top_product_write_db

    def top_ctr(**context):
        df_json = context['task_instance'].xcom_pull(task_ids='ads_active', key='data')
        df = pd.read_json(df_json)
        df = df.groupby(['date','advertiser_id', 'product_id','type']).size().reset_index(name='count')
        clicks = df[df['type'] == 'click']
        impressions = df[df['type'] == 'impression']
        # Agrupar y sumar
        clicks_sum = clicks.groupby(['date','advertiser_id', 'product_id']).agg({'count': 'sum'}).rename(columns={'count': 'clicks'})
        impressions_sum = impressions.groupby(['date','advertiser_id', 'product_id']).agg({'count': 'sum'}).rename(columns={'count': 'impressions'})
        # Unir los datos de clicks e impresiones
        result = impressions_sum.join(clicks_sum, how='left').fillna(0)
        result['clicks'] = result['clicks'].astype(float)
        result['impressions'] = result['impressions'].astype(float)
        # Calcular el CTR
        result['CTR'] = ((result['clicks'] / result['impressions'] * 100).round(2)).clip(upper=100)
        result = result.reset_index()
        print(result)
        def get_top_20_ctr(group):
            return group.sort_values('CTR', ascending=False).head(20)
        
        top_20_per_advertiser = result.groupby('advertiser_id').apply(get_top_20_ctr)
        # Debido a la naturaleza de 'apply', los índices podrían necesitar un ajuste.
        top_20_per_advertiser = top_20_per_advertiser.reset_index(drop=True)
        print(top_20_per_advertiser)
        context['task_instance'].xcom_push('data', top_20_per_advertiser.to_json())
        
       
    def write_ctr_to_db(**context):
        df_json = context['task_instance'].xcom_pull(task_ids='top_ctr', key='data')
        df = pd.read_json(df_json)
        with connect_db() as engine:
            with engine.cursor() as cursor:
                insert_query = 'INSERT INTO ctr (date, advertiser_id, product_id, impressions, clicks, CTR) VALUES (%s, %s, %s, %s, %s, %s);'
                for _, row in df.iterrows():
                    cursor.execute(insert_query, (row['date'], row['advertiser_id'], row['product_id'], row['impressions'], row['clicks'], row['CTR']))
            engine.commit()
        
    filter_data_ads = PythonOperator(
        task_id='ads_active',
        python_callable=join_csv,
        op_kwargs={'date': f"{datetime.datetime.now().strftime('%Y-%m-%d')}",
                   'file1': 'advertiser_ids.csv', 
                   'file2': 'ads_views.csv'},
        provide_context=True  # Necesario para acceder a task_instance
    )
    top_ctr_task = PythonOperator(
        task_id='top_ctr',
        python_callable=top_ctr,
        provide_context=True,  # Necesario para acceder a task_instance
    )
    write_ctr_db = PythonOperator(
        task_id='ctr_write_db',
        python_callable=write_ctr_to_db,
        provide_context=True
    )
    filter_data_ads >> top_ctr_task >> write_ctr_db

    
    
    