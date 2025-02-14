import snowflake.connector
import json
import os
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env
load_dotenv("schemas/.env")

# Debug: Print environment variables
#print("🔍 Checking environment variables:")
#print(f"USER: {os.getenv('USER')}")
#print(f"PASSWORD: {os.getenv('PASSWORD')}")  # Mask password
#print(f"ACCOUNT: {os.getenv('ACCOUNT')}")
#print(f"WAREHOUSE: {os.getenv('WAREHOUSE')}")
#print(f"DATABASE: {os.getenv('DATABASE')}")
#print(f"SCHEMA: {os.getenv('SCHEMA')}")

# Ensure ACCOUNT is not None
if not os.getenv("ACCOUNT"):
    raise ValueError("❌ ACCOUNT variable is missing or not loaded from .env")


# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD"),
    account=os.getenv("ACCOUNT"),
    warehouse=os.getenv("WAREHOUSE"),
    database=os.getenv("DATABASE"),
    schema=os.getenv("SCHEMA")
)

print("✅ Connection successful!")
cur = conn.cursor()

# Load JSON data
DATA_DIR = "../../2021q4/"
json_file_path = os.path.join(DATA_DIR, "sec_financial_data_clean.json")

with open(json_file_path, "r") as f:
    json_data = json.load(f)

# Insert JSON into Snowflake
for cik, record in json_data.items():
    company_name = record.get("company_name", None)

    for filing in record.get("filings", []):
        adsh = filing.get("adsh")
        filing_date = pd.to_datetime(filing.get("filing_date"), format="%Y%m%d").date()
        fiscal_year = int(filing.get("fiscal_year"))
        tag = filing["statement_data"].get("tag")
        value = float(filing["statement_data"].get("value"))
        unit = filing["statement_data"].get("unit")
        data = json.dumps(record)  # Store the full JSON record

        query = """
        INSERT INTO json_sec_data (cik, company_name, filing_date, fiscal_year, adsh, tag, value, unit, data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
        """
        cur.execute(query, (cik, company_name, filing_date, fiscal_year, adsh, tag, value, unit, data))


        #cur.execute(query, (cik, company_name, filing_date, fiscal_year, adsh, tag, value, unit, data))


conn.commit()
cur.close()
conn.close()

print("✅ JSON Data successfully loaded into Snowflake!")
