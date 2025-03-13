id: Financial-DB-Codelab

title: "Financial Data Pipeline Setup"

summary: "A step-by-step guide to setting up a financial data pipeline using Python, Snowflake, AWS S3, and Airflow."

authors: ["Your Name"]

categories: ["Data Engineering", "Snowflake", "AWS", "CI/CD"]

tags: ["Python", "Snowflake", "AWS", "Airflow", "Automation"]
--- 

# 🚀 Findata - Financial Statement Database Project

## 📄 Project Summary

Findata Inc. aims to build a comprehensive financial statements library that helps analysts conduct fundamental analysis on publicly traded US corporations. This project extracts, standardizes, and stores data from various sources into a unified database system.

## 📌 Overview

The Findata system provides a complete solution for financial data management:

- Snowflake for data storage and querying
- SEC Financial Statement Data Sets as the primary data source
- DBT for transforming raw financial data
- Airflow for orchestrating data pipelines
- FastAPI backend for document processing
- Streamlit web interface for user interaction
- AWS S3 for cloud storage of processed files

## 🔑 Features

- **Automated Data Ingestion**: Scrapes SEC datasets and loads them into Snowflake
- **Data Transformation**: Merges, cleans, and transforms data into JSON and normalized formats
- **Pipeline Orchestration**: Airflow manages all ETL tasks
- **Interactive Dashboard**: Streamlit app provides an intuitive interface for data exploration
- **Cloud Storage**: Processed files stored in AWS S3
- **API Access**: FastAPI backend for programmatic access to financial data
- **User-friendly Interface**: Streamlit web app for easy data visualization and analysis

## ✔ Technology Stack

| Category | Tools Used |
|----------|------------|
| Programming Language | Python 3.8+ |
| Data Platform | Snowflake |
| Workflow Automation | Apache Airflow |
| Data Transformation | DBT (Data Build Tool) |
| Backend API | FastAPI |
| Frontend UI | Streamlit |
| Cloud Storage | AWS S3 |
| Deployment | Render, Streamlit Cloud |


---

## 🚀 Installation & Setup

### 1️⃣ Prerequisites

Ensure you have:
- Python 3.8 or higher
- pip (Python package manager)
- AWS credentials (if storing files in AWS S3)

### 2️⃣ Clone the Repository & Install Dependencies

git clone https://github.com/Neneanuj/Financial_Database_Analysis

pip install -r requirements.txt

## Setup your Environment Variables
# AWS credentials
        AWS_REGION=<your_aws_region>
        AWS_ACCESS_KEY_ID=<your_access_key_id>
        AWS_SECRET_ACCESS_KEY=<your_secret_access_key>
        BUCKET_NAME=<your_bucket_name>

# Snowflake credentials
        SNOWFLAKE_ACCOUNT=<your_snowflake_account>
        SNOWFLAKE_USER=<your_snowflake_user>
        SNOWFLAKE_PASSWORD=<your_snowflake_password>
        SNOWFLAKE_DATABASE=<your_snowflake_database>
        SNOWFLAKE_SCHEMA=<your_snowflake_schema>
        SNOWFLAKE_WAREHOUSE=<your_snowflake_warehouse>
        SNOWFLAKE_TABLE=<your_snowflake_table>


# Create and activate virtual environment
        python -m venv venv
        source venv/bin/activate  (Mac)
---

### Step 2: Scrape SEC Data Links
Let's create a script to scrape the SEC website for financial statement data links:

        import boto3
        import requests
        import time
        import io
        import os
        import zipfile
        from botocore.exceptions import ClientError
        from datetime import datetime
        from dotenv import load_dotenv
        from boto3.s3.transfer import TransferConfig


        load_dotenv()
        class SECDataUploader:
            def __init__(self, bucket_name):
                self.s3_client = boto3.client('s3')
                # CHANGE:
                self.bucket_name = os.getenv('S3_BUCKET_NAME')
                self.headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive'
                }

            def generate_sec_url(self, year: int, quarter: int):
                if not (2009 <= year <= 2024):
                    raise ValueError("Year must be between 2009 and 2024")
                if not (1 <= quarter <= 4):
                    raise ValueError("Quarter must be between 1 and 4")
                
                base_url = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
                return f"{base_url}{year}q{quarter}.zip"


            def upload_to_s3(self, content: bytes, year: int, quarter: int) -> bool:
                try:
                    
                # Configure multipart upload settings   NO NOTICEABLE DIFFERENCE IN SPEED
                # config = TransferConfig(
                    #multipart_threshold=5 * 1024 * 1024,  # 5MB threshold
                    #multipart_chunksize=5 * 1024 * 1024,  # 5MB chunks
                    #max_concurrency=10,  # Number of parallel threads
                    #use_threads=True
                #)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{year}q{quarter}.zip"
                    
                    # Define S3 key (path)
                    s3_key = f"{filename}"
                    
                    # Upload to S3
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=content ,
                        ContentType = 'application/zip'
                    )
                    print(f"Uploaded {filename} to s3://{self.bucket_name}/{s3_key}")
                    return True
                    
                except ClientError as e:
                    print(f"S3 upload error: {e}")
                    return False
                

            def fetch_and_upload(self, year: int, quarter: int) -> bool:
                try:
                    url = self.generate_sec_url(year, quarter)
                    print(f"\nFetching data from: {url}")
                    
                    time.sleep(0.1)  # Rate limiting
                    response = requests.get(url, headers=self.headers)
                    
                    if response.status_code == 200 and response.content:
                        print("\nUnzipping and uploading files...")
                        return self.upload_to_s3(response.content, year, quarter)
                    else:
                        print(f"Failed to fetch data. Status code: {response.status_code}")
                        return False
                        
                except Exception as e:
                    print(f"Error: {e}")
                    return False


        def main():
            # CHANGE: Replace this with your actual S3 bucket name
            BUCKET_NAME = os.getenv('S3_BUCKET_NAME') 
            
            # CHANGE: You can also set the bucket name using environment variables
            # import os
            # BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
            
            uploader = SECDataUploader(BUCKET_NAME)
            
            try:
                year = int(input("Enter year (2009-2024): "))
                quarter = int(input("Enter quarter (1-4): "))
                
                if uploader.fetch_and_upload(year, quarter):
                    print("\nAll files successfully uploaded to S3")
                else:
                    print("\nFailed to complete the upload process")
                    
            except ValueError as e:
                print(f"Input error: {e}")

if __name__ == "__main__":
    main()
---

### Step 3: Load Raw Data to Snowflake

        def load_file_to_snowflake(self, file_path: str, table_name: str):
                try:
                        # Get file from S3
                        response = self.s3_client.get_object(
                            Bucket=self.bucket_name, 
                            Key=file_path
                        )
                        
                        # Read file content
                        file_content = response['Body'].read().decode('utf-8')
                        csv_file = io.StringIO(file_content)
                        csv_reader = csv.reader(csv_file, delimiter='\t')
                        
                        # Get headers
                        headers = next(csv_reader)
                        
                        # Prepare cursor and SQL
                        cursor = self.snowflake_conn.cursor()
                        
                        # Create INSERT statement with NULL handling
                        columns = ','.join(headers)
                        placeholders = ','.join(['NULLIF(%s,\'\')'] * len(headers))
                        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                        
                        # Load data in batches
                        batch_size = 10000
                        batch = []
                        rows_loaded = 0
                        
                        for row in csv_reader:
                            batch.append(row)
                            if len(batch) >= batch_size:
                                cursor.executemany(insert_sql, batch)
                                rows_loaded += len(batch)
                                batch = []
                        
                        # Insert remaining rows
                        if batch:
                            cursor.executemany(insert_sql, batch)
                            rows_loaded += len(batch)
                        
                        cursor.close()
                        return True, rows_loaded
                            
                except Exception as e:
                        print(f"Error loading {file_path}: {str(e)}")
                        return False, 0
---

### Step 4: Setting Up DBT for Financial Statement Data
DBT (Data Build Tool) is essential for transforming and validating our SEC financial statement data. Let's implement a comprehensive approach for both normalized and denormalized tables.

Step 1: Initialize DBT Project Structure
First, we need to set up our DBT project with the appropriate directory structure.

# Create a new DBT project
        dbt init sec_financial_data
        cd sec_financial_data

# Create necessary directories
        mkdir -p models/staging
        mkdir -p models/intermediate
        mkdir -p models/marts/core
        mkdir -p models/marts/finance
        mkdir -p tests/generic
        mkdir -p tests/singular
        Step 2: Configure DBT Profile
        Configure your DBT profile to connect to Snowflake.

text
# ~/.dbt/profiles.yml
        sec_financial_data:
        target: dev
        outputs:
            dev:
            type: snowflake
            account: [account_id]
            user: [username]
            password: [password]
            role: [role]
            database: SEC_FINANCIAL_DATA
            warehouse: [warehouse]
            schema: dbt_dev
            threads: 4
            prod:
            type: snowflake
            account: [account_id]
            user: [username]
            password: [password]
            role: [role]
            database: SEC_FINANCIAL_DATA
            warehouse: [warehouse]
            schema: dbt_prod
            threads: 8

# Step 3: Define Source Tables in YAML
Create a source definition file to map the raw tables in Snowflake.

# Step 4: Create Staging Models
Create staging models to clean and validate the raw data.
---

### Step 5: Develop DBT Transformation Strategy Document
Create a comprehensive document outlining the DBT transformation strategy.

text
# DBT Transformation Strategy Document

## Model Organization

Our DBT project follows a layered architecture:

1. **Sources**: Raw data from Snowflake tables
2. **Staging**: Cleaned and validated raw data
3. **Intermediate**: Normalized tables with business logic
4. **Marts**: Denormalized fact tables for analytics

## Testing Strategy

We implement four levels of testing:

1. **Schema Validation**: Ensuring data types and structures match expectations
2. **Data Quality**: Checking for nulls, uniqueness, and accepted values
3. **Referential Integrity**: Verifying relationships between tables
4. **Business Logic**: Custom tests for financial calculations and balances

## Implementation Approach

1. **Idempotent Transformations**: All models can be run multiple times with the same result
2. **Incremental Models**: For large tables to optimize processing
3. **Documentation**: All models and columns are documented
4. **Modularity**: Breaking complex transformations into smaller, reusable pieces
---

### Step 6: Operational Pipeline with Airflow
Setting Up Airflow for Data Processing
Now, let's implement Airflow pipelines for data validation and staging using S3 for intermediate storage


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
            USE SCHEMA DBT_DB.DBT_SCHEMA;
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
            bash_command='cd dbt/data_pipeline && dbt run --profiles-dir . --target dev',
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

