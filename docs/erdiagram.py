from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.workflow import Airflow
from diagrams.programming.framework import FastAPI
from diagrams.programming.language import Python

with Diagram("SEC Financial Data Pipeline", show=False, direction="LR"):
    # Data Sources
    with Cluster("Data Sources"):
        sec_data = Python("SEC Market Data\nScraper")
        financial_statements = Python("Financial Statements\nParser")

    # Storage Layer
    with Cluster("S3 Storage Options"):
        with Cluster("Raw Storage"):
            raw_bucket = S3("Raw Data")
        
        with Cluster("JSON Storage"):
            json_bucket = S3("JSON Transformed")
        
        with Cluster("Normalized Storage"):
            norm_bucket = S3("Denormalized Facts")

    # Snowflake Layer
    with Cluster("Snowflake"):
        with Cluster("Raw Tables"):
            raw_tables = PostgreSQL("SUB, TAG\nNUM, PRE")
        
        with Cluster("JSON Tables"):
            json_tables = PostgreSQL("Financial\nStatements JSON")
        
        with Cluster("Fact Tables"):
            fact_tables = PostgreSQL("Balance Sheet\nIncome Statement\nCash Flow")

    # DBT Transformation
    with Cluster("DBT Layer"):
        staging = Python("Staging Models")
        validation = Python("Data Validation")
        transformation = Python("Transformations")

    # Airflow Pipelines
    with Cluster("Airflow Orchestration"):
        raw_dag = Airflow("Raw Pipeline")
        json_dag = Airflow("JSON Pipeline")
        norm_dag = Airflow("Normalized Pipeline")

    # Application Layer
    with Cluster("Application"):
        api = FastAPI("FastAPI Backend")
        streamlit = Python("Streamlit Frontend")

    # Initial Data Flow
    sec_data >> Edge(color="black", style="bold") >> financial_statements
    financial_statements >> Edge(color="black", style="bold") >> raw_bucket
    
    # Raw Pipeline (Blue)
    raw_bucket >> Edge(color="blue", style="bold") >> raw_dag
    raw_dag >> Edge(color="blue", style="bold") >> raw_tables
    raw_tables >> Edge(color="blue", style="bold") >> staging
    
    # JSON Pipeline (Green)
    json_bucket >> Edge(color="green", style="bold") >> json_dag
    json_dag >> Edge(color="green", style="bold") >> json_tables
    json_tables >> Edge(color="green", style="bold") >> staging
    
    # Normalized Pipeline (Red)
    norm_bucket >> Edge(color="red", style="bold") >> norm_dag
    norm_dag >> Edge(color="red", style="bold") >> fact_tables
    fact_tables >> Edge(color="red", style="bold") >> staging
    
    # Common Flow
    staging >> Edge(color="black", style="bold") >> validation
    validation >> Edge(color="black", style="bold") >> transformation
    
    # Application Flow
    transformation >> Edge(color="black", style="bold") >> api
    api >> Edge(color="black", style="bold") >> streamlit
