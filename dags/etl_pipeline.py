from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

FILE_PATH = "/home/aya/airflow-project-linux/data/Sales-Export_2019-2020.csv"

def extract():
    df = pd.read_csv(FILE_PATH)
    df.to_csv("/tmp/raw_data.csv", index=False)

def transform():
    df = pd.read_csv("/tmp/raw_data.csv")
    
    df.columns = df.columns.str.strip()
    
    df["order_value_EUR"] = df["order_value_EUR"].replace(",", "", regex=True).astype(float)
    df["cost"] = df["cost"].replace(",", "", regex=True).astype(float)
    
    df = df.dropna()
    
    df["profit"] = df["order_value_EUR"] - df["cost"]
    
    df.to_csv("/tmp/clean_data.csv", index=False)

def load():
    df = pd.read_csv("/tmp/clean_data.csv")
    print(df.head())

with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task
