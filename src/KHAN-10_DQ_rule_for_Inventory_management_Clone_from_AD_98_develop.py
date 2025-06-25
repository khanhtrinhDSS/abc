spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for data quality checks on drug_inventory_management table
# Purpose: Implement data quality checks based on the requirements specified in the DQ_rules_IM sheet
# Author: Khanh Trinh
# Date: 2025-06-25
# Description: This script performs data quality checks including mandatory fields check, expiry date check, distinct value check, and data consistency check on the drug_inventory_management table.

try:
    # Import necessary PySpark SQL functions and types
    from pyspark.sql.functions import col, when, expr, count, sum, lit  
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
    def mandatory_fields_check(df):
        """
        Check for null values in mandatory fields.

        Args:
            df (DataFrame): The DataFrame to check.

        Returns:
            bool: True if pass, False otherwise.
        """
        mandatory_fields = ["product_ID", "product_name", "quantity", "location", "expiry_date", "batch_number", "supplier_ID"]
        null_check_results = df.select(
            *[(col(field).isNotNull().alias(field)) for field in mandatory_fields]
        ).agg(
            *[sum(when(col(field), 0).otherwise(1)).alias(field + "_null_count") for field in mandatory_fields]
        )
        return all([null_check_results.first()[field + "_null_count"] == 0 for field in mandatory_fields])

    mandatory_check_passed = mandatory_fields_check(drug_inventory_df)

    # Expiry Date Check
    def expiry_date_check(df):
        """
        Check if expiry_date is greater than purchase_date.

        Args:
            df (DataFrame): The DataFrame to check.

        Returns:
            bool: True if pass, False otherwise.
        """
        expiry_date_check_results = df.filter(col("expiry_date") > col("purchase_date"))
        return expiry_date_check_results.count() == df.count()

    expiry_date_check_passed = expiry_date_check(drug_inventory_df)

    # Distinct Value Check
    def distinct_value_check(df):
        """
        Check for distinct values in product_ID and batch_number.

        Args:
            df (DataFrame): The DataFrame to check.

        Returns:
            bool: True if pass, False otherwise.
        """
        distinct_check_results = df.groupBy("product_ID", "batch_number").count().filter("count > 1")
        return distinct_check_results.count() == 0

    distinct_check_passed = distinct_value_check(drug_inventory_df)

    # Data Consistency Check
    def data_consistency_check(df):
        """
        Check if quantity is positive and product_ID starts with "P".

        Args:
            df (DataFrame): The DataFrame to check.

        Returns:
            bool: True if pass, False otherwise.
        """
        consistency_check_results = df.filter(
            (col("quantity") > 0) & (col("product_ID").startswith("P"))
        )
        return consistency_check_results.count() == df.count()

    consistency_check_passed = data_consistency_check(drug_inventory_df)

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
