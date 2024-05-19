import datetime
import numpy as np
import pandas as pd
import boto3
import os
import psycopg2
from datetime import timedelta
from io import StringIO
from dotenv import load_dotenv 
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='advertiser_recomendations',
    schedule='0 0 * * *',
    start_date=datetime.datetime(2024, 4, 1),
    catchup=False,
) as dag:
    
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

    def write_csv_to_s3(df, file_name):
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3.put_object(Bucket=os.getenv("BUCKET_NAME"), Key=file_name, Body=csv_buffer.getvalue())
            print(f"File {file_name} saved to S3 successfully.")
        except Exception as e:
            print(f"An error occurred while writing the file {file_name} to S3: {e}")

    def join_csv(file1, file2, **context):
        df1 = read_csv_from_s3(file1)
        df2 = read_csv_from_s3(file2)
        try:
            df = pd.merge(df1, df2, on='advertiser_id')
            date = (context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
            # Filtrar por fecha
            df = df[df['date'] == date]
            df = df.reset_index(drop=True)
            file2 = file2.split(".")[0]
            file_name = f"{file2}_{date}.csv"
            write_csv_to_s3(df, file_name)
            print(df.head())
        except Exception as e:
            print(f"An error occurred while joining the DataFrames: {e}")

    def get_top_20_products(group):
        return group.sort_values('count', ascending=False).head(20)

    def calculate_product_counts(df):
        return df.groupby(['date', 'advertiser_id', 'product_id']).size().reset_index(name='count')

    def top_product(**context):
        df = read_csv_from_s3(f"product_views_{(context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')}.csv")
        result = calculate_product_counts(df)
        top_20_per_advertiser = result.groupby('advertiser_id').apply(get_top_20_products)
        top_20_per_advertiser = top_20_per_advertiser.reset_index(drop=True)
        write_csv_to_s3(top_20_per_advertiser, f"top_products_{context['execution_date'].strftime('%Y-%m-%d')}.csv")
        print(top_20_per_advertiser)

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
        df = read_csv_from_s3(f"ads_views_{(context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')}.csv")
        result = calculate_ctr(df)
        print(result)
        top_20_per_advertiser = result.groupby('advertiser_id').apply(get_top_20_ctr)
        top_20_per_advertiser = top_20_per_advertiser.reset_index(drop=True)
        write_csv_to_s3(top_20_per_advertiser, f"top_ctr_{(context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')}.csv")
        print(top_20_per_advertiser)
       
        
    def write_product_to_db(**context):
        df = read_csv_from_s3(f"top_products_{(context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')}")
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
        df = read_csv_from_s3(f"top_ctr_{(context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')}")
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

    filter_data_product = PythonOperator(
        task_id='product_active',
        python_callable=join_csv,
        op_kwargs={ 
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
        op_kwargs={
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

    
    
    