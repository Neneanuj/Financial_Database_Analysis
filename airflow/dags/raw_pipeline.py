from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import requests
import zipfile
import io
import os

class SECDataPipeline:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = Variable.get('S3_BUCKET_NAME')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        }

    def download_and_upload_sec_data(self, year: int, quarter: int):
        base_url = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
        url = f"{base_url}{year}q{quarter}.zip"
        
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            zip_content = response.content
            folder_name = f"{year}q{quarter}"
            
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
                for file_name in zip_ref.namelist():
                    with zip_ref.open(file_name) as file:
                        content = file.read()
                        s3_key = f"{folder_name}/{file_name}"
                        self.s3_client.put_object(
                            Bucket=self.bucket_name,
                            Key=s3_key,
                            Body=content
                        )
            return True
        return False

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sec_raw_data_pipeline',
    default_args=default_args,
    description='SEC Financial Data Raw Pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    def download_sec_data(**context):
        year = context['dag_run'].conf.get('year')
        quarter = context['dag_run'].conf.get('quarter')
        pipeline = SECDataPipeline()
        return pipeline.download_and_upload_sec_data(year, quarter)

    # Task 1: Download and Upload SEC Data
    fetch_sec_data = PythonOperator(
        task_id='fetch_sec_data',
        python_callable=download_sec_data,
        provide_context=True
    )

    # Task 2: Create Snowflake Stage
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE SCHEMA Fin.raw_schema;
        CREATE STAGE IF NOT EXISTS sec_raw_stage
        URL = 's3://{{ var.value.S3_BUCKET_NAME }}'
        STORAGE_INTEGRATION = s3_int
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\t' SKIP_HEADER = 1);
        """
    )

    # Task 3: Create Tables
    create_tables = SnowflakeOperator(
        task_id='create_tables',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS SUB (
            adsh VARCHAR,
            cik INTEGER,
            name VARCHAR,
            form_type VARCHAR,
            period DATE,
            filed_date DATE,
            PRIMARY KEY (adsh)
        );
        
        CREATE TABLE IF NOT EXISTS NUM (
            adsh VARCHAR,
            tag VARCHAR,
            version VARCHAR,
            data_date DATE,
            value FLOAT,
            unit_of_measure VARCHAR,
            FOREIGN KEY (adsh) REFERENCES SUB(adsh)
        );
        """
    )

    # Task 4: Create Snowflake Pipes
    create_pipes = SnowflakeOperator(
        task_id='create_pipes',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE PIPE sub_pipe AUTO_INGEST=TRUE AS
        COPY INTO SUB FROM @sec_raw_stage/{{var.value.year}}q{{var.value.quarter}}/
        PATTERN='.*sub\.txt';
        
        CREATE OR REPLACE PIPE num_pipe AUTO_INGEST=TRUE AS
        COPY INTO NUM FROM @sec_raw_stage/{{var.value.year}}q{{var.value.quarter}}/
        PATTERN='.*num\.txt';
        """
    )

    # Task 5: Monitor Loading
    monitor_loading = SnowflakeOperator(
        task_id='monitor_loading',
        snowflake_conn_id='snowflake_conn',
        sql="""
        SELECT TABLE_NAME, COUNT(*) as row_count,
        MAX(SYSTEM$PIPE_STATUS('sub_pipe')) as sub_pipe_status,
        MAX(SYSTEM$PIPE_STATUS('num_pipe')) as num_pipe_status
        FROM (
            SELECT 'SUB' as TABLE_NAME, COUNT(*) as cnt FROM SUB
            UNION ALL
            SELECT 'NUM' as TABLE_NAME, COUNT(*) as cnt FROM NUM
        ) GROUP BY TABLE_NAME;
        """
    )

    # Set task dependencies
    fetch_sec_data >> create_stage >> create_tables >> create_pipes >> monitor_loading
