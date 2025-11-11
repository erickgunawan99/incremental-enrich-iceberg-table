# ===== TASK 1: GX VALIDATION =====
from datetime import datetime
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession

from pyspark.sql.functions import date_format, from_unixtime, col, year


from great_expectations.dataset import SparkDFDataset

def validate_with_gx(**context):
    execution_date = context['ds']
    
    spark = SparkSession.builder \
        .appName("GX_Validation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    manifest_df = spark.read.text('s3a://transaction/pending/manifest.pending')
    paths = [row.value for row in manifest_df.select("value").collect()]
    df = spark.read.parquet(*paths)
  
    df_transform = df.withColumn("orderdate", from_unixtime(col("orderdate"))) \
                       .withColumn("orderdate_date", date_format("orderdate", "yyyy-MM-dd")) \
                       .withColumn("orderdate_time", date_format("orderdate", "HH:mm:ss")) \
                       .withColumn("orderdate_year", year("orderdate"))
    
    # Simple approach - wrap the DataFrame directly
    validator = SparkDFDataset(df_transform, expectation_suite_name="minio_orders_suite")
  
    # Define expectations
    validator.expect_table_row_count_to_be_between(min_value=500, max_value=500)
    validator.expect_column_values_to_not_be_null(column="invoiceid")
    validator.expect_column_values_to_match_regex(column="orderdate_date", regex=r"^\d{4}-\d{2}-\d{2}$")
    validator.expect_column_values_to_be_between(column="orderdate_year", min_value=2020, max_value=2025)

    # Validate and get results
    results = validator.validate()

    # Calculate metrics
    total_expectations = len(results.results)
    passed_expectations = sum(1 for r in results.results if r.success)
    pass_rate = (passed_expectations / total_expectations) * 100
    
    validation_summary = {
        'success': results.success,
        'total_expectations': total_expectations,
        'passed_expectations': passed_expectations,
        'failed_expectations': total_expectations - passed_expectations,
        'pass_rate': pass_rate,
        'record_count': df.count(),
        'execution_date': execution_date,
        'validation_timestamp': datetime.now().strftime("%Y-%m-%d:%H:%M:%S")
    }
    
    print(f"""
    ═══════════════════════════════════════════
    GX VALIDATION RESULTS
    ═══════════════════════════════════════════
    Date: {execution_date}
    Records: {validation_summary['record_count']:,}
    Pass Rate: {pass_rate:.2f}%
    ═══════════════════════════════════════════
    """)
    
    # Save audit log
    results_df = spark.createDataFrame([validation_summary])
    results_df.write.mode("append").parquet(
        f"s3a://audit-logs/gx-validations/date={execution_date}/"
    )
    
    spark.stop()
    
    return validation_summary

# ===== TASK 2: DECISION GATE =====
def check_validation_threshold(**context):
    """
    Check if GX validation passed threshold.
    Returns task_id to execute next.
    """
    # Pull validation results from XCom
    validation_results = context['task_instance'].xcom_pull(
        task_ids='validate_with_gx'
    )
    
    # Define your thresholds
    MIN_PASS_RATE = 100  # 100% of expectations must pass
    MIN_RECORD_COUNT = 500  # At least 500 records
    
    pass_rate = validation_results['pass_rate']
    record_count = validation_results['record_count']
    
    print(f"Checking thresholds:")
    print(f"  Pass Rate: {pass_rate:.2f}% (threshold: {MIN_PASS_RATE}%)")
    print(f"  Record Count: {record_count:,} (threshold: {MIN_RECORD_COUNT:,})")
    
    # Decision logic
    if pass_rate >= MIN_PASS_RATE and record_count >= MIN_RECORD_COUNT:
        print("✅ Thresholds met! Proceeding to Spark ETL job.")
        return 'spark_job'  # Task ID to execute
    else:
        print("❌ Thresholds NOT met! Moving data to quarantine.")
        return 'quarantine_data'  # Task ID to execute
    
# ===== TASK 3B: QUARANTINE DATA (if validation failed) =====
def quarantine_bad_data(**context):
    execution_date = datetime.now().strftime("%Y-%m-%d:%H:%M:%S")

    spark = SparkSession.builder \
        .appName("Quarantine_Data") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    # Read the problematic data
    manifest_df = spark.read.text('s3a://transaction/pending/manifest.pending')
    paths = [row.value for row in manifest_df.select("value").collect()]
    df = spark.read.parquet(*paths)
    
    # Move to quarantine with timestamp
    quarantine_path = f"s3a://quarantine/transaction/failed_validation/{execution_date}/"
    df.write.mode("append").parquet(quarantine_path)
    
    print(f"⚠️ Data quarantined to: {quarantine_path}")
    
    
    spark.stop()
