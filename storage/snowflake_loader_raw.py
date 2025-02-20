import os
import time
from dotenv import load_dotenv
import snowflake.connector
import boto3
import csv
import io

class SnowflakeLoader:
    def __init__(self):
        load_dotenv()
        self.snowflake_conn = self._connect_to_snowflake()
        self.s3_client = self._connect_to_s3()
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
   
    def _connect_to_snowflake(self):
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )

    def _connect_to_s3(self):
        return boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
        )

    def get_file_paths(self, year: int, quarter: int):
        if not (2009 <= year <= 2024):
            raise ValueError("Year must be between 2009 and 2024")
        if not (1 <= quarter <= 4):
            raise ValueError("Quarter must be between 1 and 4")

        period = f"{year}q{quarter}"
        return {
            'SUB': f"{period}/sub.txt",
            'TAG': f"{period}/tag.txt",
            'NUM': f"{period}/num.txt",
            'PRE': f"{period}/pre.txt"
        }

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



    #def load_file_to_snowflake(self, file_path: str, table_name: str):
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
            placeholders = ','.join(['%s'] * len(headers))
            insert_sql = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})"
            
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

    def process_quarter(self, year: int, quarter: int):
        files = self.get_file_paths(year, quarter)
        results = {}
        
        for table, file_path in files.items():
            print(f"Processing {table} from {file_path}")
            success, rows = self.load_file_to_snowflake(file_path, table)
            results[table] = {'success': success, 'rows': rows}
        
        return results

    def close(self):
        if self.snowflake_conn:
            self.snowflake_conn.close()

def main():
    loader = SnowflakeLoader()
    
    try:
        year = int(input("Enter year (2009-2024): "))
        quarter = int(input("Enter quarter (1-4): "))
        
        results = loader.process_quarter(year, quarter)
        
        for table, result in results.items():
            status = "Success" if result['success'] else "Failed"
            print(f"{table}: {status} - {result['rows']} rows loaded")
            
    except ValueError as e:
        print(f"Input error: {e}")
    except Exception as e:
        print(f"Processing error: {e}")
    finally:
        loader.close()

if __name__ == "__main__":
    main()
