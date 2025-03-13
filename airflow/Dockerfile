FROM apache/airflow:2.8.1-python3.11

USER airflow

# ⚠️ pip 必須用 --user，protobuf/dbt 安裝順序正確
RUN pip install --user --no-cache-dir "protobuf>=5.0,<6.0"
RUN pip install --user --no-cache-dir "dbt-core==1.9.3" "dbt-snowflake==1.9.2"
