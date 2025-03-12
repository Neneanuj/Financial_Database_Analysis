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

    #def unzip_and_upload(self, zip_content: bytes, year: int, quarter: int) -> bool:
        try:
            zip_buffer = io.BytesIO(zip_content)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # CHANGE: 
            folder_name = f"{year}Q{quarter}"       #Removed Timestamp and Raw_data
            
            with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    with zip_ref.open(file_name) as file:
                        content = file.read()
                        s3_key = f"{folder_name}/{file_name}"
                        self.s3_client.put_object(
                            Bucket=self.bucket_name,
                            Key=s3_key,
                            Body=content
                        )
                        #print(f"Uploaded {file_name} to s3://{self.bucket_name}/{s3_key}")
            
            return True
            
        except ClientError as e:
            #print(f"S3 upload error: {e}")
            return False
        except zipfile.BadZipFile as e:
            #print(f"Zip file error: {e}")
            return False

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
