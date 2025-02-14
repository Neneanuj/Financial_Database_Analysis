import pandas as pd
import json
import os
import zipfile
# Define data directory
DATA_DIR = "../../2021q4/"
ZIP_FILE = os.path.join(DATA_DIR, "2021q4.zip")
EXTRACT_DIR = os.path.join(DATA_DIR, "extracted/")  # Folder to store .txt files
OUTPUT_FILE = os.path.join(DATA_DIR, "sec_financial_data_clean.json")

def unzip_sec_data():
    """Unzips SEC financial data and extracts .txt files."""
    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)

    with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)
    
    print(f"✅ Extracted SEC data to {EXTRACT_DIR}")
'''
def read_sec_csv(filename):
    """Reads an SEC financial statement CSV file and returns a Pandas DataFrame."""
    file_path = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(file_path, sep="\t", dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    print(f"🔍 Columns in {filename}: {df.columns.tolist()}")
    return df
'''
def read_sec_csv(filename):
    """Reads an SEC financial statement CSV file and returns a Pandas DataFrame."""
    file_path = os.path.join(DATA_DIR, filename)
    
    try:
        df = pd.read_csv(file_path, sep="\t", dtype=str)
        df.columns = df.columns.str.strip().str.lower()
        print(f"🔍 Columns in {filename}: {df.columns.tolist()}")
        
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"File {filename} not found in {EXTRACT_DIR}. Ensure the data has been extracted.")


def clean_data(df, file_type):
    """Cleans SEC financial statement data."""
    # Apply different rules for sub.txt and num.txt
    if file_type == "sub":
        required_columns = ["adsh", "cik", "fy", "filed"]  # No 'tag' or 'value' here
    elif file_type == "num":
        required_columns = ["adsh", "tag", "value"]  # No 'cik', 'filed', 'fy' here
    else:
        raise ValueError("Invalid file type specified.")
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"Missing required columns: {missing_columns}")
    
    # Drop rows with missing key fieldss
    df.dropna(subset=required_columns, inplace=True)

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df[df["value"] >= 0]  # Remove negative values
    

    # Convert 'value' to float
    #df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Convert 'filed' to YYYY-MM-DD format
    #df["filed"] = pd.to_datetime(df["filed"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")

    # Remove negative values if they don't make sense (e.g., total assets)
    #df = df[df["value"] >= 0]
    # Remove duplicates
    df.drop_duplicates(inplace=True)

    return df
'''
def clean_data(df):
    """Cleans SEC financial statement data."""
    df.dropna(subset=["adsh", "cik", "filed", "fy", "tag", "value"], inplace=True)
    df.drop_duplicates(inplace=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["filed"] = pd.to_datetime(df["filed"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["value"] >= 0]  # Remove invalid negative values
    return df
'''
def transform_to_json(sub_df, num_df):
    """Merges SEC submission & numeric data into JSON."""
    json_data = {}
    # Clean data
    sub_df = clean_data(sub_df, "sub")  # Pass "sub" for sub.txt data 
    num_df = clean_data(num_df, "num")  # Pass "num" for num.txt data
    # Ensure required columns exist before merging
    required_sub_columns = ["adsh", "cik", "name", "filed", "fy"]
    required_num_columns = ["adsh", "tag", "value", "uom"]
    
    missing_sub_columns = [col for col in required_sub_columns if col not in sub_df.columns]
    missing_num_columns = [col for col in required_num_columns if col not in num_df.columns]

    if missing_sub_columns or missing_num_columns:
        raise KeyError(f"Missing columns - sub_df: {missing_sub_columns}, num_df: {missing_num_columns}")

    merged_df = sub_df.merge(num_df, on="adsh", how="inner")


    for cik, group in merged_df.groupby("cik"):
        json_data[cik] = {
            "company_name": group["name"].iloc[0],
            "filings": []
        }
        for _, row in group.iterrows():
            filing_data = {
                "adsh": row["adsh"],
                "filing_date": row["filed"],
                "fiscal_year": row["fy"],
                "statement_data": {
                    "tag": row["tag"],
                    "value": row["value"],
                    "unit": row["uom"]
                }
            }
            json_data[cik]["filings"].append(filing_data)
    
    return json_data

if __name__ == "__main__":
    # Unzip data first
    unzip_sec_data()
    sub_df = read_sec_csv("extracted/sub.txt")
    num_df = read_sec_csv("extracted/num.txt")
    
    
    print(num_df.columns)
    json_data = transform_to_json(sub_df, num_df)

    json_file_path = os.path.join(DATA_DIR, "sec_financial_data_clean.json")
    with open(json_file_path, "w") as f:
        json.dump(json_data, f, indent=4)

    print(f"✅ JSON saved to {json_file_path}")

