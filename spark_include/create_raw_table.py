from pyspark.sql import SparkSession

def create_raw_table():
    spark = SparkSession.builder \
        .appName("createRawTable") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://transaction/warehouse") \
        .config("spark.sql.catalog.my_catalog.s3.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sql("""CREATE OR REPLACE TABLE my_catalog.db.raw_transaction_iceberg (
    invoiceid BIGINT,
    itemid BIGINT,
    customerid BIGINT,
    category STRING,
    price DOUBLE,
    quantity INT,
    orderdate TIMESTAMP,       
    processed_date TIMESTAMP, 
    state STRING,
    shippingtype STRING,
    referral STRING
)
USING iceberg
PARTITIONED BY (
    months(orderdate)
);
""")

    spark.stop()

def main():
    create_raw_table()

if __name__ == "__main__":
    main()