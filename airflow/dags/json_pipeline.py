from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from airflow.providers.snowflake.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta
import os
import json
#from storage.snowflake_loader import load_json_to_snowflake

#def process_json():
#    load_json_to_snowflake("../../2021q4/sec_financial_data_clean.json", "json_sec_data")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'json_pipeline',
    default_args=default_args,
    description='JSON Financial Data Pipeline DAG',
    schedule_interval='@daily',
    catchup=False
)

# 1. Basic Python Task
def print_context(**context):
    """Shows how to use context variables in Airflow"""
    print(f"Execution date is {context['ds']}")
    print(f"Task instance: {context['task_instance']}")
    return "Hello from first Json task!"

task1 = PythonOperator(
    task_id='print_context',
    python_callable=print_context, #process_json
    dag=dag
)

# 1. Unzip SEC data
def unzip_data(**context):
    os.system("unzip -o ../../2021q4/2021q4.zip -d ../../2021q4/extracted/")
    print("✅ Data unzipped successfully")

unzip_task = PythonOperator(
    task_id='unzip_sec_data',
    python_callable=unzip_data,
    dag=dag
)
# 2. Transform SEC CSV to JSON
def transform_to_json(**context):
    os.system("python data_ingestion/sec_csv_reader.py")
    print("✅ JSON transformation complete")

transform_task = PythonOperator(
    task_id='transform_to_json',
    python_callable=transform_to_json,
    dag=dag
)

# 3. Load JSON into Snowflake
def load_to_snowflake(**context):
    os.system("python storage/snowflake_loader_json.py")
    print("✅ JSON loaded into Snowflake")

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)
# 4. create table
create_table = SnowflakeOperator(
    task_id='create_json_table',
    snowflake_conn_id='snowflake_default',
    sql="""
    USE SCHEMA Fin.JSON_SCHEMA;
    CREATE TABLE IF NOT EXISTS json_sec_data (
        cik STRING,
        company_name STRING,
        filing_date DATE,
        fiscal_year INT,
        adsh STRING,
        tag STRING,
        value FLOAT,
        unit STRING,
        data VARIANT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# 5. Run DBT transformations
run_dbt = BashOperator(
    task_id='run_dbt_models',
    #bash_command='cd dbt/data_pipeline && dbt run --profiles-dir . --target dev',
     bash_command="""
        echo "👉 Running dbt..."
        cd /opt/airflow/dbt/data_pipeline
        pwd
        ls -la
        dbt debug --profiles-dir .
        dbt run --profiles-dir . --target dev
    """,
    
    dag=dag
)

# 6. Run DBT tests
test_dbt = BashOperator(
    task_id='test_dbt_models',
    bash_command='cd dbt/data_pipeline && dbt test --profiles-dir . --target dev',
    dag=dag
)


# Task dependencies
unzip_task >> transform_task >> create_table >> load_task >> run_dbt >> test_dbt

