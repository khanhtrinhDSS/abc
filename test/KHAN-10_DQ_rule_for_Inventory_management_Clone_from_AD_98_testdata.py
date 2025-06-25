# PySpark script for data quality checks on drug_inventory_management table
# Purpose: Implement data quality checks based on the requirements specified in the DQ_rules_IM sheet
# Author: Khanh Trinh
# Date: 2025-06-25
# Description: This script performs data quality checks including mandatory fields check, expiry date check, distinct value check, and data consistency check on the drug_inventory_management table.

try:
    # Import necessary PySpark SQL functions and types
    from pyspark.sql.functions import col, when, count, sum  
    from pyspark.sql.types import StringType, TimestampType, LongType, StructType, StructField  

    # Define schema for data quality results DataFrame
    dq_schema = StructType([
        StructField("check_name", StringType(), True),
        StructField("result", StringType(), True),
        StructField("pass_%", LongType(), True)
    ])

    # Read the drug_inventory_management table
    drug_inventory_df = spark.table("purgo_playground.drug_inventory_management")

    # Mandatory Fields Check
    # Ensure all fields are not null
    mandatory_fields = ["product_ID", "product_name", "quantity", "location", "expiry_date", "batch_number", "supplier_ID"]
    null_check_results = drug_inventory_df.select(
        *[(col(field).isNotNull().alias(field)) for field in mandatory_fields]
    ).agg(
        *[sum(when(col(field), 0).otherwise(1)).alias(field + "_null_count") for field in mandatory_fields]
    )

    mandatory_check_passed = null_check_results.first().isNotNull()

    # Expiry Date Check
    expiry_date_check_results = drug_inventory_df.filter(col("expiry_date") > col("purchase_date"))
    expiry_date_check_passed = expiry_date_check_results.count() == drug_inventory_df.count()

    # Distinct Value Check
    distinct_check_results = drug_inventory_df.groupBy("product_ID", "batch_number").count().filter("count > 1")
    distinct_check_passed = distinct_check_results.count() == 0

    # Data Consistency Check
    consistency_check_results = drug_inventory_df.filter(
        (col("quantity") > 0) & (col("product_ID").startswith("P"))
    )
    consistency_check_passed = consistency_check_results.count() == drug_inventory_df.count()

    # Create DataFrame with data quality check results
    dq_results_df = spark.createDataFrame([
        ("Mandatory Fields Check", "Passed" if mandatory_check_passed else "Failed", 100 * mandatory_check_passed),
        ("Expiry Date Check", "Passed" if expiry_date_check_passed else "Failed", 100 * expiry_date_check_passed),
        ("Distinct Value Check", "Passed" if distinct_check_passed else "Failed", 100 * distinct_check_passed),
        ("Data Consistency Check", "Passed" if consistency_check_passed else "Failed", 100 * consistency_check_passed)
    ], schema=dq_schema)

    # Display the result of data quality checks
    dq_results_df.show()

except Exception as e:
    print(f"An error occurred: {e}")
