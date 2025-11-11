from pyspark.sql import SparkSession
import os

# Set Hadoop conf dir if needed
# os.environ["HADOOP_CONF_DIR"] = "C:\\spark\\conf"
# ../include/hadoop-aws-3.3.4.jar,../include/hadoop-common-3.3.4.jar,../include/hadoop-client-3.3.4.jar,
def main():
    spark = SparkSession.builder \
        .appName("EnrichToIceberg") \
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


    spark.sparkContext.setLogLevel("WARN")

    # Read the grouped CSV
    df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://dimension/us_states_dim1.csv")
    df.printSchema()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.db")
    #Write to Iceberg table (create if not exists)
    df.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("my_catalog.db.dim_us_states")

    print("Written to Iceberg table my_catalog.db.dim_us_states")
    spark.stop()


if __name__ == "__main__":
    main()
