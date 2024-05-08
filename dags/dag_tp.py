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

load_dotenv()

def connect_db():
    return psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"), 
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

s3 = boto3.client(
    's3',
    region_name='us-east-2',
    aws_access_key_id= os.getenv("ACCESS_KEY"),
    aws_secret_access_key= os.getenv("SECRET_KEY")  
)

def read_csv_from_s3(file_name):
    try:
        file = s3.get_object(Bucket = os.getenv("BUCKET_NAME"), Key = file_name)
        df = pd.read_csv(file['Body'])
        return df
    except Exception as e:
        print(f"An error occurred while reading the file {file_name} from S3: {e}")
        return pd.DataFrame()

def join_csv(date, file1, file2, **context):
    df1 = read_csv_from_s3(file1)
    df2 = read_csv_from_s3(file2)
    # Unir los DataFrames
    try:
        df = pd.merge(df1, df2, on='advertiser_id')
        # Filtrar por fecha
        df = df[df['date'] == date]
        print(df.head())
        # Empujar el DataFrame en XComs
        context['task_instance'].xcom_push('data', df.to_json())
    except Exception as e:
        print(f"An error occurred while joining the DataFrames: {e}")

def get_top_20_products(group):
    return group.sort_values('count', ascending=False).head(20)

def calculate_product_counts(df):
    return df.groupby(['date', 'advertiser_id', 'product_id']).size().reset_index(name='count')

def top_product(**context):
    df_json = context['task_instance'].xcom_pull(task_ids='product_active', key='data')
    df = pd.read_json(df_json)
    result = calculate_product_counts(df)
    top_20_per_advertiser = result.groupby('advertiser_id').apply(get_top_20_products)
    top_20_per_advertiser = top_20_per_advertiser.reset_index(drop=True)
    print(top_20_per_advertiser)
    context['task_instance'].xcom_push('data', top_20_per_advertiser.to_json())

def get_top_20_ctr(group):
    return group.sort_values('CTR', ascending=False).head(20)

def calculate_ctr(df):
    df = df.groupby(['date','advertiser_id', 'product_id','type']).size().reset_index(name='count')
    clicks = df[df['type'] == 'click']
    impressions = df[df['type'] == 'impression']
    clicks_sum = clicks.groupby(['date','advertiser_id', 'product_id']).agg({'count': 'sum'}).rename(columns={'count': 'clicks'})
    impressions_sum = impressions.groupby(['date','advertiser_id', 'product_id']).agg({'count': 'sum'}).rename(columns={'count': 'impressions'})
    result = impressions_sum.join(clicks_sum, how='left').fillna(0)
    result['clicks'] = result['clicks'].astype(float)
    result['impressions'] = result['impressions'].astype(float)
    result['CTR'] = ((result['clicks'] / result['impressions'] * 100).round(2)).clip(upper=100)
    return result.reset_index()

def top_ctr(**context):
    df_json = context['task_instance'].xcom_pull(task_ids='ads_active', key='data')
    df = pd.read_json(df_json)
    result = calculate_ctr(df)
    print(result)
    top_20_per_advertiser = result.groupby('advertiser_id').apply(get_top_20_ctr)
    top_20_per_advertiser = top_20_per_advertiser.reset_index(drop=True)
    print(top_20_per_advertiser)
    context['task_instance'].xcom_push('data', top_20_per_advertiser.to_json())
    
def write_product_to_db(**context):
    df_json = context['task_instance'].xcom_pull(task_ids='top_product', key='data')
    df = pd.read_json(df_json)
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                insert_query = 'INSERT INTO product (date, advertiser_id, product_id, count) VALUES (%s, %s, %s, %s);'
                check_query = 'SELECT 1 FROM product WHERE date = %s AND advertiser_id = %s AND product_id = %s;'
                for _, row in df.iterrows():
                    cursor.execute(check_query, (row['date'], row['advertiser_id'], row['product_id']))
                    if cursor.fetchone() is not None:
                        print(f"Data for date {row['date']}, advertiser_id {row['advertiser_id']}, and product_id {row['product_id']} already exists.")
                        continue
                    cursor.execute(insert_query, (row['date'], row['advertiser_id'], row['product_id'], row['count']))
            engine.commit()
    except psycopg2.OperationalError as e:
        print(f"An error occurred while connecting to the database: {e}")

def write_ctr_to_db(**context):
    df_json = context['task_instance'].xcom_pull(task_ids='top_ctr', key='data')
    df = pd.read_json(df_json)
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                insert_query = 'INSERT INTO ctr (date, advertiser_id, product_id, impressions, clicks, CTR) VALUES (%s, %s, %s, %s, %s, %s);'
                check_query = 'SELECT 1 FROM ctr WHERE date = %s AND advertiser_id = %s AND product_id = %s;'
                for _, row in df.iterrows():
                    cursor.execute(check_query, (row['date'], row['advertiser_id'], row['product_id']))
                    if cursor.fetchone() is not None:
                        print(f"Data for date {row['date']}, advertiser_id {row['advertiser_id']}, and product_id {row['product_id']} already exists.")
                        continue
                    cursor.execute(insert_query, (row['date'], row['advertiser_id'], row['product_id'], row['impressions'], row['clicks'], row['CTR']))
            engine.commit()
    except psycopg2.OperationalError as e:
        print(f"An error occurred while connecting to the database: {e}")

with DAG(
    dag_id='filtrar_datos',
    schedule='0 7 * * *',
    start_date=datetime.datetime(2024, 4, 1),
    catchup=False,
) as dag:

    filter_data_product = PythonOperator(
        task_id='product_active',
        python_callable=join_csv,
        op_kwargs={'date': f"{datetime.datetime.now().strftime('%Y-%m-%d')}", 
                'file1' : 'advertiser_ids.csv', 
                'file2' : 'product_views.csv'},
        provide_context=True  
    )

    top_product_task = PythonOperator(
        task_id='top_product',
        python_callable=top_product,
        provide_context=True,  
    )

    top_product_write_db = PythonOperator(
        task_id='product_write_db',
        python_callable=write_product_to_db,
        provide_context=True
    )
    
    filter_data_ads = PythonOperator(
        task_id='ads_active',
        python_callable=join_csv,
        op_kwargs={'date': f"{datetime.datetime.now().strftime('%Y-%m-%d')}",
                   'file1': 'advertiser_ids.csv', 
                   'file2': 'ads_views.csv'},
        provide_context=True  
    )
    top_ctr_task = PythonOperator(
        task_id='top_ctr',
        python_callable=top_ctr,
        provide_context=True,  
    )
    write_ctr_db = PythonOperator(
        task_id='ctr_write_db',
        python_callable=write_ctr_to_db,
        provide_context=True
    )
    filter_data_product >> top_product_task >> top_product_write_db
    filter_data_ads >> top_ctr_task >> write_ctr_db

    
    
    