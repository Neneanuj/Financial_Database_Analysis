import os
import zipfile
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import argparse
import csv


# Import SECDataUploader from scraper.py (ensure scraper.py is in the same directory or package)
from sec_scraper_fact import SECDataUploader

# Load environment variables (e.g., S3_BUCKET_NAME and AWS credentials)
load_dotenv()

# ----------------------------
# Directory Configuration: All data is stored under ../tmp_data
# ----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tmp_data"))
ZIP_FOLDER = BASE_DIR  # Directory where ZIP files are stored
os.makedirs(ZIP_FOLDER, exist_ok=True)

# ----------------------------
# S3 Download Class: Used to download ZIP files from S3 to local storage
# ----------------------------
class SECDataDownloader:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name

    def download_file(self, s3_key: str, local_dir: str) -> str:
        """
        Downloads a file from S3 based on the provided S3 key and saves it to the specified local directory.
        Returns the local file path.
        """
        local_file_path = os.path.join(local_dir, s3_key)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
            print(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_file_path}")
            return local_file_path
        except ClientError as e:
            print(f"Error downloading file: {e}")
            return None

# ----------------------------
# Directly Parse SEC Data and Generate Denormalized Fact Tables (No Intermediate JSON)
# ----------------------------
def process_sec_data_direct(zip_filepath: str, ticker_filepath: str = None):
    """
    从本地 ZIP 文件中直接读取 SEC 数据，合并 ticker 映射后生成三个 DataFrame：
      - 资产负债表 (stmt == 'BS')
      - 收益表 (stmt == 'IS')  注意：这里使用 'IS'，因为示例中收益表使用的是 "IS"
      - 现金流量表 (stmt == 'CF')
    最终每个表包含字段：ticker, cik, filing_date, fiscal_year, fiscal_period, tag, value, uom, plabel
    """
    # 如果未指定 ticker_filepath，默认放在ZIP文件同一目录下
    if ticker_filepath is None:
        ticker_filepath = os.path.join(os.path.dirname(zip_filepath), "ticker.txt")

    # 读取 ZIP 文件内的SEC数据文件
    with zipfile.ZipFile(zip_filepath, 'r') as z:
        # 注意：实际文件结构可能不同，这里假设文件名为 num.txt, pre.txt, sub.txt, tag.txt
        dfNum = pd.read_table(z.open('num.txt'), delimiter="\t")
        dfPre = pd.read_table(z.open('pre.txt'), delimiter="\t")
        dfSub = pd.read_table(z.open('sub.txt'), delimiter="\t", low_memory=False)
        dfTag = pd.read_table(z.open('tag.txt'), delimiter="\t")

    # 读取 ticker.txt 文件：第一列为 ticker, 第二列为 cik
    if os.path.exists(ticker_filepath):
        dfTicker = pd.read_table(ticker_filepath, delimiter="\t", header=None, names=['ticker', 'cik'])
    else:
        print(f"Warning: ticker file not found at {ticker_filepath}")
        dfTicker = pd.DataFrame(columns=['ticker', 'cik'])

    # 标准化数据类型：
    # 将 ticker 文件中的 cik 转换为字符串，并去掉前后空格
    dfTicker['cik'] = dfTicker['cik'].astype(str).str.strip()

    # 对SEC数据中的 cik 也做同样处理，假设 cik 来自 dfSub
    if 'cik' in dfSub.columns:
        dfSub['cik'] = dfSub['cik'].astype(str).str.strip()

    # 将数值文件中的 value 列转换为数字，并删除无法转换的记录
    dfNum['value'] = pd.to_numeric(dfNum['value'], errors='coerce')
    dfNum = dfNum.dropna(subset=['value'])
    dfNum['value'] = dfNum['value'].astype(float)

    # 进行数据合并
    # 首先，将 dfNum 和 dfPre 按 ['adsh', 'tag'] 进行左合并
    dfMerged = pd.merge(dfNum, dfPre, on=['adsh', 'tag'], how='left')
    # 再将合并结果与 dfSub 按 'adsh' 进行左合并（dfSub 包含 cik 字段）
    dfMerged = pd.merge(dfMerged, dfSub, on='adsh', how='left', suffixes=('', '_sub'))
    # 最后将 dfMerged 与 dfTicker 按 'cik' 进行左合并，将 ticker 加入到结果中
    dfMerged = pd.merge(dfMerged, dfTicker, on='cik', how='left', suffixes=('', '_ticker'))

    # 调试输出：查看前几行合并结果中 cik 与 ticker 的匹配情况
    print("Merged ticker mapping sample:")
    print(dfMerged[['cik', 'ticker']].head())

    # 如果合并后产生重复列（例如 ticker_ticker），则将其用于填充 ticker 列
    if 'ticker_ticker' in dfMerged.columns:
        dfMerged['ticker'] = dfMerged['ticker'].combine_first(dfMerged['ticker_ticker'])
    
    # 处理日期和期间信息：
    # 将 'period' 转换为 filing_date，这里转换为标准格式 YYYY-MM-DD
    dfMerged['filing_date'] = pd.to_datetime(dfMerged['period'], errors='coerce')
    # 如果转换失败（比如原始数据仅有年份），则以年初作为默认日期
    dfMerged['filing_date'] = dfMerged['filing_date'].fillna(
        pd.to_datetime(dfMerged['period'].astype(int).astype(str) + '-01-01', errors='coerce')
    )
    dfMerged['filing_date'] = dfMerged['filing_date'].dt.strftime('%Y-%m-%d')
    # 提取 fiscal_year 和 fiscal_period（直接使用原始字段）
    dfMerged['fiscal_year'] = dfMerged['fy']
    dfMerged['fiscal_period'] = dfMerged['fp']

    # 将 stmt 字段转换为大写，以便于忽略大小写进行过滤
    dfMerged['stmt'] = dfMerged['stmt'].str.upper()

    # 根据 stmt 过滤不同报表
    dfBS = dfMerged[dfMerged['stmt'] == 'BS'].copy()
    dfIC = dfMerged[dfMerged['stmt'] == 'IS'].copy()  # 使用 'IS' 表示收益表（根据你的数据示例）
    dfCF = dfMerged[dfMerged['stmt'] == 'CF'].copy()

    # 选取最终需要的列，注意：第一列必须为 ticker
    required_cols = ['ticker', 'cik', 'filing_date', 'fiscal_year', 'fiscal_period',
                     'tag', 'value', 'uom']

    def select_cols(df, cols):
        return df[[col for col in cols if col in df.columns]]

    dfBS = select_cols(dfBS, required_cols)
    dfIC = select_cols(dfIC, required_cols)
    dfCF = select_cols(dfCF, required_cols)

    # 调试输出，查看每个报表 DataFrame 的前几行
    print("Balance Sheet sample:")
    print(dfBS.head())
    print("Income Statement sample:")
    print(dfIC.head())
    print("Cash Flow sample:")
    print(dfCF.head())

    return dfBS, dfIC, dfCF


# ----------------------------
# Function to Upload CSV Files to S3
# ----------------------------
def upload_csv_to_s3(local_path, s3_key, bucket_name):
    """
    Uploads a CSV file to S3.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading {local_path}: {e}")
        raise e

# Global variables for user input
global_year = None
global_quarter = None

# ----------------------------
# Main Process: Upload → Download → Parse ZIP → Generate CSV → Upload to S3
# ----------------------------
def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Process SEC data for a specific year and quarter.')
    parser.add_argument('--year', type=int, required=True, help='Year (2009-2024)')
    parser.add_argument('--quarter', type=int, required=True, help='Quarter (1-4)')
    args = parser.parse_args()

    # 使用解析后的参数
    global_year = args.year
    global_quarter = args.quarter

    bucket_name = os.getenv('S3_BUCKET_NAME')
    if not bucket_name:
        print("Please set S3_BUCKET_NAME in your environment variables.")
        return

    # Step 1: Upload SEC data to S3 using SECDataUploader
    uploader = SECDataUploader(bucket_name)
    if not (2009 <= global_year <= 2024):
        raise ValueError("Year must be between 2009 and 2024")
    if not (1 <= global_quarter <= 4):
        raise ValueError("Quarter must be between 1 and 4")

    if uploader.fetch_and_upload(global_year, global_quarter):
        print("\nFiles successfully uploaded to S3.")
    else:
        print("\nFailed to upload files to S3.")
        return

    # Step 2: Download ZIP file from S3
    downloader = SECDataDownloader(bucket_name)
    s3_key = f"{global_year}q{global_quarter}.zip"  # Assuming S3 key follows this format
    local_zip = downloader.download_file(s3_key, ZIP_FOLDER)
    if not local_zip:
        print("Failed to download ZIP file from S3.")
        return

    # Step 3: Parse the ZIP file and generate three fact table DataFrames
    dfBS, dfIC, dfCF = process_sec_data_direct(local_zip)

    # Step 4: Save the DataFrames as CSV files
    bs_csv = os.path.join(BASE_DIR, "balance_sheet.csv")
    ic_csv = os.path.join(BASE_DIR, "income_statement.csv")
    cf_csv = os.path.join(BASE_DIR, "cash_flow.csv")
    dfBS.to_csv(bs_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    dfIC.to_csv(ic_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    dfCF.to_csv(cf_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)

    print("\nCSV files generated:")
    print(f"  - {bs_csv}")
    print(f"  - {ic_csv}")
    print(f"  - {cf_csv}")

    # Step 5: Upload CSV files to S3
    folder_name = f"{global_year}q{global_quarter}/fact_tables"
    upload_csv_to_s3(bs_csv, f"{folder_name}/balance_sheet.csv", bucket_name)
    upload_csv_to_s3(ic_csv, f"{folder_name}/income_statement.csv", bucket_name)
    upload_csv_to_s3(cf_csv, f"{folder_name}/cash_flow.csv", bucket_name)

    print("\nIngestion and extraction process completed.")

if __name__ == "__main__":
    main()
