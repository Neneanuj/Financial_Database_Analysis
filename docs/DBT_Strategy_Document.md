# 📝 DBT Strategy Document

## **📌 Overview**  
This document outlines the DBT strategy for the Findata Inc. Financial Statement Database Project, detailing the models, transformations, and best practices adopted.

---

## **🛠️DBT Project Structure**

```plaintext
findata/
├── dbt_transform/
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   │   ├── raw/              # Raw staging
│   │   │   ├── json/             # JSON staging
│   │   │   └── normalized/       # Normalized staging
│   │   └── marts/                # Business models
│   ├── tests/                    # Data quality checks
│   └── docs/strategy.md          # DBT strategy document
```

---

## **🛠️DBT Models**
1. Staging Models

* Raw Staging: Ingest raw SEC data from Snowflake.
* JSON Staging: Process and flatten JSON data for easier querying.
* Normalized Staging: Transform raw data into normalized tables.

2. Marts (Business Models)

* Combine staging models into final business models:
    *  balance_sheet_mart
    *  income_statement_mart
    *  cash_flow_martc

---

## **🛠️Best Practices**
* Modular Design: Separate raw, intermediate, and final models.
* Testing: Implement schema and data quality checks.
* Version Control: Use GitHub for DBT model versioning.

---

## **🛠️DBT Pipelines in Airflow**
* Raw Pipeline: Extract and load raw SEC data.
* JSON Pipeline: Transform and load JSON data.
* Normalized Pipeline: Load normalized tables and marts.

---

## **🛠️Performance Optimization**
* Incremental Models: Use incremental loads for large datasets.
* Materializations: Employ ephemeral, table, and view materializations appropriately.
* Testing and CI/CD: Integrate with Airflow for automated testing.