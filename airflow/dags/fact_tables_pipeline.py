from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import os
import sys
import subprocess
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_ingestion.sec_scraper_fact import SECDataUploader



# 默认参数设置
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    
    'fact_tables_pipeline',
    default_args=default_args,
    description='Extract SEC fact tables, upload CSVs to S3, create stage and load data into Snowflake in DBT_DB.DBT_SCHEMA',
    schedule_interval='@daily',
    catchup=False
)

# 1. 数据提取任务：调用外部脚本 fact_tables_extracted.py
def run_ingestion_extraction(**kwargs):
    # Retrieve parameters from dag_run.conf or use defaults
    conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    year = conf.get("year", 2021)
    quarter = conf.get("quarter", 4)
    
    # Get the absolute path for fact_tables_extracted.py.
    # Since data_ingestion is now inside the airflow folder, we go up one level from dags.
    current_dir = os.path.dirname(os.path.abspath(__file__))  # e.g., /opt/airflow/dags
    script_path = os.path.abspath(os.path.join(current_dir, "..", "data_ingestion", "fact_tables_extracted.py"))
    
    if not os.path.exists(script_path):
        raise Exception(f"Script not found: {script_path}")
    
    print(f"Script path: {script_path}")
    
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.path.join(current_dir, ".."))

    cmd = f"python '{script_path}' --year {year} --quarter {quarter}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
    print(f"Running command: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print("stdout:", result.stdout)
    print("stderr:", result.stderr)
    if result.returncode != 0:
        raise Exception(f"Command failed with return code {result.returncode}")
    print(f"Ingestion and extraction completed for {year}q{quarter}.")

ingestion_task = PythonOperator(
    task_id='run_ingestion_extraction',
    python_callable=run_ingestion_extraction,
    provide_context=True,
    dag=dag,
)

# 2. 创建 S3 Stage：指向存储 CSV 文件的 S3 桶 (bigdata-project2-storage)
create_stage = SQLExecuteQueryOperator(
    task_id='create_s3_stage',
    conn_id='snowflake_default',
    sql="""
    USE SCHEMA Fin.fact_tables;
    CREATE STAGE IF NOT EXISTS my_s3_stage
    URL='s3://bigdata-project2-storage/'
    CREDENTIALS=(AWS_KEY_ID='{{ conn.aws_default.login }}'
                 AWS_SECRET_KEY='{{ conn.aws_default.password }}');
    """,
    dag=dag,
)

# 3. 在 Snowflake 中创建目标表（在 DBT_DB.DBT_SCHEMA 下）
create_balance_sheet = SQLExecuteQueryOperator(
    task_id='create_balance_sheet',
    conn_id='snowflake_default',
    sql="""
    USE SCHEMA DBT_DB.DBT_SCHEMA;
    CREATE OR REPLACE TABLE balance_sheet (
        ticker STRING,
        cik STRING,
        filing_date DATE,
        fiscal_year INT,
        fiscal_period STRING,
        tag STRING,
        value FLOAT,
        unit STRING
    );
    """,
    dag=dag,
)

create_income_statement = SQLExecuteQueryOperator(
    task_id='create_income_statement',
    conn_id='snowflake_default',
    sql="""
    USE SCHEMA DBT_DB.DBT_SCHEMA;
    CREATE OR REPLACE TABLE income_statement (
        ticker STRING,
        cik STRING,
        filing_date DATE,
        fiscal_year INT,
        fiscal_period STRING,
        tag STRING,
        value FLOAT,
        unit STRING
    );
    """,
    dag=dag,
)

create_cash_flow = SQLExecuteQueryOperator(
    task_id='create_cash_flow',
    conn_id='snowflake_default',
    sql="""
    USE SCHEMA DBT_DB.DBT_SCHEMA;
    CREATE OR REPLACE TABLE cash_flow (
        ticker STRING,
        cik STRING,
        filing_date DATE,
        fiscal_year INT,
        fiscal_period STRING,
        tag STRING,
        value FLOAT,
        unit STRING
    );
    """,
    dag=dag,
)

# 4. 从 S3 Stage 读取 CSV 数据到 Snowflake表
# 使用 Jinja 模板动态构造 S3 路径，根据 dag_run.conf 中传入的 year 和 quarter（如果未传入则使用默认值 2021 和 4）
load_balance_sheet = SQLExecuteQueryOperator(
    task_id='load_balance_sheet',
    conn_id='snowflake_default',
    sql="""
    COPY INTO balance_sheet
    FROM '@my_s3_stage/{{ dag_run.conf.year | default(2021) }}q{{ dag_run.conf.quarter | default(4) }}/fact_tables/balance_sheet.csv'
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag,
)

load_income_statement = SQLExecuteQueryOperator(
    task_id='load_income_statement',
    conn_id='snowflake_default',
    sql="""
    COPY INTO income_statement
    FROM '@my_s3_stage/{{ dag_run.conf.year | default(2021) }}q{{ dag_run.conf.quarter | default(4) }}/fact_tables/income_statement.csv'
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag,
)

load_cash_flow = SQLExecuteQueryOperator(
    task_id='load_cash_flow',
    conn_id='snowflake_default',
    sql="""
    COPY INTO cash_flow
    FROM '@my_s3_stage/{{ dag_run.conf.year | default(2021) }}q{{ dag_run.conf.quarter | default(4) }}/fact_tables/cash_flow.csv'
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag,
)

# 设置任务依赖关系
ingestion_task >> create_stage >> [create_balance_sheet, create_income_statement, create_cash_flow]
create_balance_sheet >> load_balance_sheet
create_income_statement >> load_income_statement
create_cash_flow >> load_cash_flow
