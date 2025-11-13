from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F


def create_spark_session():
    try:
        builder = SparkSession.builder.appName("IcbergIncrementalWriter") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
             .config("spark.sql.ui.explainMode", "extended") \
             .config("spark.sql.adaptive.enabled", "true") \
             .config("spark.sql.adaptive.logLevel", "info") \
             .config("spark.shuffle.partitions", "24") 
        spark = builder.getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("spark session created")
        return spark
    except Exception as e:
        print(f"cant create spark session -> {e}")

def read_manifest(spark):
    try:
        manifest_df = spark.read.text('s3a://transaction/pending/manifest.pending')
        return manifest_df
    except Exception as e:
        print(f"cant read manifest -> {e}")
        raise

def read_parquet_data(spark, manifest_df):
    try:
        paths = [row.value for row in manifest_df.select("value").collect()]
        data_df = spark.read.parquet(*paths)
        return data_df
    except Exception as e:
        print(f"cant read parquet -> {e}")
        raise

def transform_df(data_df: DataFrame) -> DataFrame:
    try:
        data_df = data_df.withColumn("orderdate", to_timestamp(col("orderdate")))
        data_df = data_df.withColumn("processed_date", current_timestamp())
        data_df = data_df.repartition(4, "orderdate") \
            .sortWithinPartitions("orderdate")
        return data_df
    except Exception as e:
        print(f"cant read parquet -> {e}")
        raise
    
def write_iceberg(spark, df):
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.db")
        # Write to Iceberg table partitioned by order_day
        df.write.format("iceberg") \
            .mode("append") \
            .saveAsTable("my_catalog.db.raw_transaction_iceberg")
        print("data written to iceberg table successfully")
        df.printSchema()
        df.show(5, truncate=False)
    except Exception as e:
        print(f"fail to write to iceberg table -> {e}")
        raise

def main():
    try:
        spark = create_spark_session()  # only create it once
        manifest_df = read_manifest(spark)
        data_df = read_parquet_data(spark, manifest_df)
        data_df = transform_df(data_df)
        write_iceberg(spark, data_df)
        spark.stop()
    except Exception as e:
        spark.stop()
        print(e)
        raise

if __name__ == '__main__':
    main()