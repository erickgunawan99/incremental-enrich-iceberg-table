from pyspark.sql import SparkSession

def merge():
    spark = SparkSession.builder.appName("Merge") \
        .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024) \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.ui.explainMode", "extended") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.logLevel", "info") \
        .config("spark.shuffle.partitions", "24") \
        .getOrCreate()
  
    new_rows_df = spark.sql("""
         SELECT *
         FROM my_catalog.db.raw_transaction_iceberg
         WHERE processed_date > (
             COALESCE((SELECT MAX(processed_date) 
                       FROM my_catalog.db.enriched_transaction_by_state),
                      '1990-01-01 00:00:00')
          )
            """)
    
    # Register the view for MERGE
    new_rows_df.createOrReplaceTempView("new_rows")
    count_new = new_rows_df.count()
    print(f"Number of new rows to merge: {count_new}")

    spark.sql("""
            MERGE INTO my_catalog.db.enriched_transaction_by_state t
    USING (
        SELECT /*+ BROADCAST(d) */
            r.invoiceid,
            r.itemid,
            r.customerid,
            r.category,
            r.price,
            r.quantity,
            r.orderdate,
            r.processed_date,
            r.state,
            r.shippingtype,
            r.referral,
            d.`Employment.Nonemployer Establishments` AS Employment_Nonemployer_Establishments,
            d.`Housing.Households` AS Housing_Households,
            d.`Housing.Housing Units` AS Housing_Housing_Units,
            d.`Housing.Median Value of Owner-Occupied Units` AS Housing_Median_Value_of_Owner_Occupied_Units,
            d.`Miscellaneous.Manufacturers Shipments` AS Miscellaneous_Manufacturers_Shipments,
            d.`Miscellaneous.Veterans` AS Miscellaneous_Veterans,
            d.`Population.2020 Population` AS Population_2020_Population,
            d.`Population.2010 Population` AS Population_2010_Population,
            d.`Sales.Accommodation and Food Services Sales` AS Sales_Accommodation_and_Food_Services_Sales,
            d.`Sales.Retail Sales` AS Sales_Retail_Sales,
            d.`Employment.Firms.Total` AS Employment_Firms_Total,
            d.`Employment.Firms.Women-Owned` AS Employment_Firms_Women_Owned,
            d.`Employment.Firms.Men-Owned` AS Employment_Firms_Men_Owned,
            d.`Employment.Firms.Minority-Owned` AS Employment_Firms_Minority_Owned,
            d.`Employment.Firms.Nonminority-Owned` AS Employment_Firms_Nonminority_Owned,
            d.`Employment.Firms.Veteran-Owned` AS Employment_Firms_Veteran_Owned,
            d.`Employment.Firms.Nonveteran-Owned` AS Employment_Firms_Nonveteran_Owned,
            d.`Income.Median Houseold Income` AS Income_Median_Household_Income,
            d.`Income.Per Capita Income` AS Income_Per_Capita_Income
        FROM new_rows r
        LEFT JOIN my_catalog.db.dim_us_states d
            ON r.state = d.state_abbreviation
    ) s
    ON t.invoiceid = s.invoiceid
    WHEN NOT MATCHED THEN INSERT (
        invoiceid,
        itemid,
        customerid,
        category,
        price,
        quantity,
        orderdate,
        processed_date,
        state,
        shippingtype,
        referral,
        State_Employment_Nonemployer_Establishments,
        State_Housing_Households,
        State_Housing_Housing_Units,
        State_Housing_Median_Value_of_Owner_Occupied_Units,
        State_Miscellaneous_Manufacturers_Shipments,
        State_Miscellaneous_Veterans,
        State_Population_2020_Population,
        State_Population_2010_Population,
        State_Sales_Accommodation_and_Food_Services_Sales,
        State_Sales_Retail_Sales,
        State_Employment_Firms_Total,
        State_Employment_Firms_Women_Owned,
        State_Employment_Firms_Men_Owned,
        State_Employment_Firms_Minority_Owned,
        State_Employment_Firms_Nonminority_Owned,
        State_Employment_Firms_Veteran_Owned,
        State_Employment_Firms_Nonveteran_Owned,
        State_Income_Median_Houseold_Income,
        State_Income_Per_Capita_Income
    )
    VALUES (
        s.invoiceid,
        s.itemid,
        s.customerid,
        s.category,
        s.price,
        s.quantity,
        s.orderdate,
        s.processed_date,
        s.state,
        s.shippingtype,
        s.referral,
        s.Employment_Nonemployer_Establishments,
        s.Housing_Households,
        s.Housing_Housing_Units,
        s.Housing_Median_Value_of_Owner_Occupied_Units,
        s.Miscellaneous_Manufacturers_Shipments,
        s.Miscellaneous_Veterans,
        s.Population_2020_Population,
        s.Population_2010_Population,
        s.Sales_Accommodation_and_Food_Services_Sales,
        s.Sales_Retail_Sales,
        s.Employment_Firms_Total,
        s.Employment_Firms_Women_Owned,
        s.Employment_Firms_Men_Owned,
        s.Employment_Firms_Minority_Owned,
        s.Employment_Firms_Nonminority_Owned,
        s.Employment_Firms_Veteran_Owned,
        s.Employment_Firms_Nonveteran_Owned,
        s.Income_Median_Household_Income,
        s.Income_Per_Capita_Income
    )
    """)

    # Stop the Spark session
    spark.stop()

def main():
    merge()

if __name__ == "__main__":
    main()

