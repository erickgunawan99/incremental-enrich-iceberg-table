**This project demonstrates a production-grade Data Lakehouse architecture using Apache Iceberg, Apache Spark, and Airflow. It features automated data quality enforcement with Great Expectations (GX) and an incremental enrichment pattern.**
1. Pre-Orchestration Setup (Bootstrap Phase): Before the Airflow DAG takes over, the environment is initialized to establish the Lakehouse schema and reference data.
    Key Setup Steps:

    1. Infrastructure: Spinning up the stack via Docker Compose (Spark, Airflow, MinIO).

    2. Data Generation: Seeding the raw bucket in MinIO with initial transaction data.

    3. Schema Initialization: Running a one-time Spark script to:

    4. Create the raw Iceberg table.

    5. Ingest country_demographics.csv into a permanent dimension Iceberg table.

    6. Initialize the enriched output table where the final joined results will reside.
    <img width="2816" height="1536" alt="Gemini_Generated_Image_t36pvgt36pvgt36p" src="https://github.com/user-attachments/assets/5b3a25f3-b242-4609-9319-f0ec200fc7be" />

2. Airflow Orchestration Workflow: The DAG manages the incremental lifecycle of data, from ingestion to archiving, with a strict "Quality-First" gate.

     Operational Flow:
     1. Manifest Creation: Identifies a specific batch of new files (e.g., max 5 files) to process, ensuring predictable resource usage.
     2.  Data Quality Gate (Great Expectations): Validates the batch against critical business rules:
         * Date Formats: Ensures consistency across time columns.
         * Range Validation: Validates that years fall within logical bounds.
         * Value Constraints: Checks min/max values for numeric fields to prevent data skew or corruption
     3.  Branching Logic
          + Success Path: If data passes the threshold, spark_job writes to the raw Iceberg table, and merge_incremental_iceberg performs an Upsert/Merge into the final                 enriched         table.
          + Quarantine Path: If validation fails, data is moved to a quarantine zone for manual inspection, bypassing the Iceberg warehouse.
     4. Cleanup & Archiving: Successfully processed files are moved to an archive folder, and the manifest is deleted to prepare for the next hourly run.
  
        <img width="2816" height="1536" alt="Gemini_Generated_Image_t36pvgt36pvgt36p (1)" src="https://github.com/user-attachments/assets/21f99dff-0014-4c8d-960b-c9e174a91117" />

4. Technical Configuration Highlights: The pipeline utilizes a Hadoop-style Iceberg Catalog stored in MinIO.

   The Spark configuration is optimized for stability and  observability:
   + Iceberg Catalog: Configured via spark.sql.catalog.my_catalog using the hadoop type.
   + S3A Integration: Custom S3A settings (endpoint, access keys, and path-style access) allow Spark to treat MinIO as an S3-compatible backend.
   + Monitoring: Event logging is enabled to allow the Spark History Server to visualize job performance and query execution plans.
   + Fault Tolerance: Uses specific Airflow trigger_rules (e.g., none_failed_min_one_success) to ensure the DAG converges correctly regardless of whether data was processed     or quarantined.
