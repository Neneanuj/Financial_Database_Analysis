from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from data_transform.fact_tables_transform import transform_to_fact_tables
from storage.snowflake_loader import load_data_to_snowflake

def fact_tables_transformation(**kwargs):
    bucket_name = 'bigdata-project2-storage'
    # 从 dag_run conf 获取 JSON 文件名，例如 "2021q1.json"
    dag_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    json_key = dag_conf.get('json_key')
    if not json_key:
        raise ValueError("json_key 参数必须在 dag_run conf 中提供。")
    
    # 转换为三张报表数据
    balance_sheet, income_statement, cash_flow = transform_to_fact_tables(bucket_name, json_key)
    
    # 加载数据到 Snowflake，表名请根据实际情况调整
    load_data_to_snowflake("BALANCE_SHEET_TABLE", balance_sheet)
    load_data_to_snowflake("INCOME_STATEMENT_TABLE", income_statement)
    load_data_to_snowflake("CASH_FLOW_TABLE", cash_flow)
    
    return {
        "balance_sheet_count": len(balance_sheet),
        "income_statement_count": len(income_statement),
        "cash_flow_count": len(cash_flow)
    }

default_args = {
    'start_date': datetime(2023, 1, 1)
}

with DAG('fact_tables_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    fact_tables_task = PythonOperator(
        task_id='fact_tables_transformation',
        python_callable=fact_tables_transformation,
        provide_context=True
    )
