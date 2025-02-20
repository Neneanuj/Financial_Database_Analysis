# Findata - Financial Statement Database Project 
### **📄 Project Summary**  
Demo Link: 

🚀 **Extract, Standardize, and Store Data from PDFs and Webpages**  

## **📌 Overview**
The goal of Findata Inc. is to create a consolidated library of financial statements that will assist analysts in performing fundamental analysis on publicly traded US corporations.  

The system provides:  
✅ **Snowflake** for data storage and querying
✅ **SEC Financial Statement Data Sets** for raw financial data
✅ **DBT** for data transformations
✅ **Airflow** for data pipelines and automation
✅ **FastAPI backend** for document processing  
✅ **Streamlit web interface** for user-friendly interaction  
✅ **Cloud storage (AWS S3)** for processed files  

---

## **🔑 Features**
✅ **Automated Data Ingestion:** Scrapes SEC datasets and loads them into Snowflake. 
✅ **Data Transformation:** Merges, cleans, and transforms data into JSON and normalized formats.
✅ **Pipeline Orchestration:** Airflow manages all ETL tasks.  
✅ **Interactive Dashboard:** Streamlit app provides an easy-to-use interface for data exploration. 
✅ **Store data in AWS S3**  
✅ **Provide an API with FastAPI:** FastAPI backend for programmatic access to financial data.  
✅ **User-friendly Streamlit Web App**   

---

## **✔ Technology Stack**

| **Category**       | **Tools Used** |
|------------------|--------------|
| **Programming Language** | Python 3.8+ |
| **Snowflake** | Cloud data platform for storage and queryinge. |
| **Apache Airflow** | Workflow automation and scheduling. |
| **DBT (Data Build Tool)** | Data transformations and modeling. |
| **Backend API** | FastAPI |
| **Frontend UI** | Streamlit |
| **Cloud Storage** | AWS S3 |
| **Deployment** | Render, Streamlit Cloud |

---

## **🛠️ Diagrams**

![image](./docs/data_extraction_architecture.png)


---

## **📂 Project Structure**
```plaintext
findata/
├── data_ingestion/               # Task 1: Data Ingestion
│   ├── sec_scraper.py            # SEC link scraping script
│   └── sample_dataset/           # Sample dataset files (for local testing)
│
├── storage/                      # Task 2: Storage Design
│   ├── schemas/
│   │   ├── raw_schema.sql        # Raw schema DDL
│   │   ├── json_schema.sql       # JSON schema DDL
│   │   └── normalized_schema.sql # Normalized schema DDL
│   └── snowflake_loader.py       # Unified data loader
│
├── dbt_transform/                # Task 3: DBT Transformation
│   ├── models/
│   │   ├── staging/              # Three staging models
│   │   │   ├── raw/
│   │   │   ├── json/ 
│   │   │   └── normalized/
│   │   └── marts/                # Unified business models
│   ├── tests/
│   │   ├── quality_checks.sql    # Data quality checks
│   │   └── schema_validation.yml # Schema validation
│   └── docs/strategy.md          # DBT strategy documentation
│
├── airflow/                      # Task 4: Pipelines
│   ├── dags/
│   │   ├── raw_pipeline.py       # Raw data processing DAG
│   │   ├── json_pipeline.py      # JSON transformation DAG
│   │   └── normalized_pipeline.py# Normalized data DAG
│   └── config/
│       ├── base_config.yaml      # General configuration
│       └── q4202-teamX.yaml      # Quarter-specific configuration
│
├── tests/                        # Task 5: Testing
│   ├── integration/              # Integration tests
│   ├── data_validation.py        # Data validation logic
│   └── test_report.md            # Test documentation template
│
├── app/                          # Task 6: Application Layer
│   ├── frontend/                 # Streamlit
│   │   ├── app.py
│   │   └── queries/              # Example queries for all three storage options
│   └── backend/                  # FastAPI
│       ├── api.py
│       └── snowflake_client.py   # Unified Snowflake client
│
├── docs/                         # Documentation
│   ├── ARCHITECTURE.md           # Architecture design
│   ├── TESTING.md                # Testing documentation
│   └── AI_USAGE.md               # AI usage disclosure
│
└── config/                       # Environment Configuration
    ├── snowflake_creds.json
    └── s3_conn.yml

```

---

## **🚀 Installation & Setup**
1️⃣ Prerequisites
Ensure you have:

Python 3.8 or higher
pip (Python package manager)
AWS credentials (if storing files in AWS S3)

2️⃣ Clone the Repository
```
git clone https://github.com/yourusername/findata-financial-db.git
cd findata-financial-db
```

3️⃣ Install Dependencies
```
pip install -r requirements.txt
```

4️⃣ Configure Environment
* Snowflake Credentials: Add your credentials to 
```
config/snowflake_creds.json.
```
* S3 Connection: Add your S3 details to 
```
config/s3_conn.yml.
```

5️⃣ Run Data Ingestion
```
python data_ingestion/sec_scraper.py
python data_ingestion/sec_csv_reader.py
```

6️⃣ Load Data into Snowflake
```
python storage/snowflake_loader.py
```

7️⃣ Run Airflow Pipelines

* Start Docker and run Airflow:
```
docker-compose up -d
```
* Access the Airflow web interface at http://localhost:8080.
* Trigger the DAGs for data processing.

8️⃣ Deploy and Access Applications
* Streamlit App: Run the frontend:
```
cd app/frontend
streamlit run app.py
```
* FastAPI Backend: Run the backend:
```
cd app/backend
uvicorn api:app --reload
```

9️⃣ Testing
Run tests:
```
pytest tests/
```

---

## **📌 Expected Outcomes**

* Merged SEC Data: CSV files from SEC (sub.txt, num.txt) are merged into clean, structured data.
* Clean Data: Data is cleaned, transformed, and validated using Python scripts and DVT tools.
* JSON Storage: Transformed data stored in Snowflake in JSON format.
* Automated Pipelines: Airflow pipelines handle ingestion, validation, and storage automatically.
* Accessible Applications: Streamlit app and FastAPI backend for querying and visualizing financial data.

---

## **📌 AI Use Disclosure**
This project uses:

📄 See AiUseDisclosure.md for details.

---

## **👨‍💻 Authors**
* Sicheng Bao (@Jellysillyfish13)
* Yung Rou Ko (@KoYungRou)
* Anuj Rajendraprasad Nene (@Neneanuj)

---

## **📞 Contact**
For questions, reach out via Big Data Course or open an issue on GitHub.
