from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import os
from manifest_and_archive.manifest_and_archive import create_manifest, archive_callable, quarantine_callable, delete_manifest
from validation.validate import validate_with_gx, check_validation_threshold, quarantine_bad_data
from datetime import datetime, timedelta
    
#"spark.jars": "/opt/airflow/include/hadoop-aws-3.3.4.jar,/opt/airflow/include/iceberg-spark-runtime-3.4_2.12-1.5.2.jar,/opt/airflow/include/aws-java-sdk-bundle-1.12.262.jar",
 #       "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
def build_spark_conf():
    return {
        # ---- Iceberg Catalog Config ----
        "spark.sql.catalog.my_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.my_catalog.type": "hadoop",
        "spark.sql.catalog.my_catalog.warehouse": "s3a://transaction/warehouse",

        # ---- MinIO / S3 Config ----
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
        "spark.hadoop.fs.s3a.retry.interval": "2000",
        "spark.hadoop.fs.s3a.attempts.maximum": "5",
        "spark.hadoop.fs.s3a.retry.limit": "3",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        # ---- Spark UI + Monitoring ----
        "spark.ui.enabled": "true",
        "spark.ui.port": "4040",

        # ---- Event Logging (Required for History Server) ----
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:///opt/spark-events",
        "spark.history.fs.logDirectory": "file:///opt/spark-events",

        # ---- Stability for Cluster Mode ----
        "spark.network.timeout": "300s",
        "spark.executor.heartbeatInterval": "60s",
    }


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'orders_etl_with_gx_validation',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)


create_manifest = PythonOperator(
          task_id='create_manifest',
          python_callable=create_manifest,
          op_kwargs={'max_files': 5},
            dag=dag
     )

validate_data = PythonOperator(
    task_id='validate_with_gx',
    python_callable=validate_with_gx,
    dag=dag
)


check_threshold = BranchPythonOperator(
    task_id='check_validation_threshold',
    python_callable=check_validation_threshold,
    dag=dag
)


# ===== TASK 3A: RUN SPARK (if validation passed) =====
spark_job = SparkSubmitOperator(
    task_id='spark_job',
    application='/opt/airflow/dags/spark_job/write_to_iceberg.py',
    name='write_to_iceberg',
    conn_id='spark_default',
    conf=build_spark_conf(),
    dag=dag
)

# FIXED: Added trigger_rule to handle tasks after branching
merge_incremental_iceberg = SparkSubmitOperator(
          task_id="merge_incremental_iceberg",
          application="/opt/airflow/dags/spark_job/merge.py",
          conn_id="spark_default",
          conf=build_spark_conf(),
          verbose=False,
          trigger_rule='none_failed_min_one_success',  # Run if spark_job succeeded
          dag=dag
     )

archive_processed_files = PythonOperator(
          task_id='archive_processed_files',
          python_callable=archive_callable,
          trigger_rule='none_failed_min_one_success',  # Run if merge succeeded
          dag=dag
     )

quarantine_data = PythonOperator(
    task_id='quarantine_data',
    python_callable=quarantine_callable,
    dag=dag
)

# FIXED: Added trigger_rule to handle branching convergence
delete_manifest = PythonOperator(
          task_id='delete_manifest',
          python_callable=delete_manifest,
          trigger_rule='none_failed_min_one_success',  # Run if either path completed
          dag=dag
     )

# ===== TASK 4: CONVERGENCE (for DAG visualization) =====
end_task = EmptyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',  # Succeeds if either path completes
    dag=dag,
)


# ===== DAG STRUCTURE =====
create_manifest >> validate_data >> check_threshold >> [spark_job, quarantine_data]
spark_job >> merge_incremental_iceberg >> archive_processed_files >> delete_manifest >> end_task
quarantine_data >> delete_manifest >> end_task