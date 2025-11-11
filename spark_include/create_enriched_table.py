from pyspark.sql import SparkSession
import os

def create_enriched_table():
    spark = SparkSession.builder \
        .appName("createEnrichTable") \
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

    spark.sql("""
CREATE OR REPLACE TABLE my_catalog.db.enriched_transaction_by_state (
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
    referral STRING,
    State_Employment_Nonemployer_Establishments BIGINT,
    State_Housing_Households BIGINT,
    State_Housing_Housing_Units BIGINT,
    State_Housing_Median_Value_of_Owner_Occupied_Units BIGINT,
    State_Miscellaneous_Manufacturers_Shipments BIGINT,
    State_Miscellaneous_Veterans BIGINT,
    State_Population_2020_Population BIGINT,
    State_Population_2010_Population BIGINT,
    State_Sales_Accommodation_and_Food_Services_Sales BIGINT,
    State_Sales_Retail_Sales BIGINT,
    State_Employment_Firms_Total BIGINT,
    State_Employment_Firms_Women_Owned BIGINT,
    State_Employment_Firms_Men_Owned BIGINT,
    State_Employment_Firms_Minority_Owned BIGINT,
    State_Employment_Firms_Nonminority_Owned BIGINT,
    State_Employment_Firms_Veteran_Owned BIGINT,
    State_Employment_Firms_Nonveteran_Owned BIGINT,
    State_Income_Median_Houseold_Income DOUBLE,
    State_Income_Per_Capita_Income DOUBLE
)
USING iceberg
PARTITIONED BY (months(orderdate));
""")

    spark.stop()

def main():
    create_enriched_table()

if __name__ == "__main__":
    main()


