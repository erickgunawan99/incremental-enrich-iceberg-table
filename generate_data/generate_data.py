import boto3
import boto3.session
import pyarrow as pa
import pyarrow.parquet as pq
import random
import time
from faker import Faker
import io
import datetime
import os

from dotenv import load_dotenv

load_dotenv()
# Define PyArrow Schema
schema = pa.schema([
    ("invoiceid", pa.int64()),
    ("itemid", pa.int64()),
    ("customerid", pa.int64()),
    ("category", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.int32()),
    ("orderdate", pa.int64()),  # Store as Unix timestamp (int)
    ("state", pa.string()),
    ("shippingtype", pa.string()),
    ("referral", pa.string())
])


def generate_fake_data(num_records=100):
    """Generate a batch of fake records."""
    fake = Faker()
    start = datetime.datetime(2020, 1, 1)
    end = datetime.datetime(2025, 12, 31)
    us_state_abbrs = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    safe_state_abbr = lambda: next(abbr for abbr in iter(fake.state_abbr, None) if abbr in us_state_abbrs)
    data = [
        (
            fake.unique.random_number(digits=7),
            fake.unique.random_number(digits=3),
            fake.unique.random_number(digits=5),
            fake.word(),
            round(random.uniform(10, 100), 2),
            random.randint(1, 5),
            int(fake.date_time_between(start_date=start, end_date=end).timestamp()),  # Convert to integer date
            safe_state_abbr(),
            random.choice(['2-Day', '3-Day', 'Standard']),
            fake.word()
        )
        for _ in range(num_records)
    ]
    print(data)
    return data


def write_parquet_to_memory(data):
    """Convert list of tuples into a Parquet file stored in memory."""
    table = pa.Table.from_arrays(list(zip(*data)), schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    print(table)
    return buffer


def upload_to_s3(bucket_name, s3_path, data_buffer):
    """Upload the Parquet file to S3."""
    s3 = boto3.client('s3', endpoint_url='http://localhost:9000', 
                            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"), 
                            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
                            region_name = 'us-east-1',
                            aws_session_token=None, 
                            config=boto3.session.Config(signature_version='s3v4'), 
                            verify=False) 
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=data_buffer.getvalue())
        print(f"Uploaded to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")


def generate_and_upload_data(bucket_name, num_files, num_records_per_file, s3_directory):
    """Generate fake data, convert to Parquet, and upload to S3."""
    for i in range(11, num_files + 1):
        # Reset the Faker unique state between files to avoid uniqueness errors
        Faker.seed(random.randint(1, 10000))
        data = generate_fake_data(num_records_per_file)
        parquet_buffer = write_parquet_to_memory(data)
        file_name = f"transaction{i}.parquet"
        s3_path = f"{s3_directory}/{file_name}"
        upload_to_s3(bucket_name, s3_path, parquet_buffer)
        print(f"Generated file {i}/{num_files} with {num_records_per_file} records")


if __name__ == "__main__":
    # data = generate_fake_data()
    # write_parquet_to_memory(data)
    bucket_name = "transaction"
    num_files = 15  # Number of Parquet files
    num_records_per_file = 100  # Number of records per file
    s3_directory = "raw"

    start_time = time.time()
    generate_and_upload_data(bucket_name, num_files, num_records_per_file, s3_directory)
    end_time = time.time()

    print(f"Total time taken: {end_time - start_time:.2f} seconds")
