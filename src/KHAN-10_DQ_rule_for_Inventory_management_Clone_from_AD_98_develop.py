spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for running data quality checks on purgo_playground.drug_inventory_management
# Purpose: Check for nulls, validity, distinctness and consistency as defined in DQ_rules_IM for inventory management
# Author: Khanh Trinh
# Date: 2025-06-25
# Description: This script reads the drug inventory management table and performs 4 DQ checks (mandatory fields, expiry date, distinct fields, value consistency). 
#              For each rule, it records the result and pass percentage. 
#              The resulting metrics per rule are stored in a data quality DataFrame and displayed.
#              Error handling is present for missing columns and type mismatches.

try:
    # from pyspark.sql import SparkSession  # SparkSession already available in Databricks
    from pyspark.sql.functions import col, count, sum, when, lit, expr  
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType  
    # All above are from pyspark.sql - Databricks runtime
    # No need for pip install

    ##############################################
    # Configuration and Helper Variable Section  #
    ##############################################

    # Table name (UNITY CATALOG.SCHEMA.TABLE)
    inventory_tbl = "purgo_playground.drug_inventory_management"

    # List of mandatory fields for the Mandatory Fields Check
    mandatory_fields = [
        "product_ID",
        "product_name",
        "quantity",
        "location",
        "expiry_date",
        "batch_number",
        "supplier_ID"
    ]

    # Columns required for each check
    expiry_needed = ["expiry_date", "purchase_date"]
    distinct_needed = ["product_ID", "batch_number"]
    consistency_needed = ["quantity", "product_ID"]

    #########################
    # Data Quality Checkers #
    #########################

    def get_table_with_column_check(table_name, columns):
        """
        Checks if the specified columns exist in the table schema.

        Args:
            table_name (str): The Unity Catalog table path.
            columns (List[str]): Columns to validate existence.

        Returns:
            DataFrame: The loaded Spark DataFrame if columns exist.

        Raises:
            Exception: If any required columns are missing.
        """
        df = spark.table(table_name)
        col_set = set(map(str.lower, df.columns))
        missing_cols = [c for c in columns if c.lower() not in col_set]
        if missing_cols:
            raise Exception(f"Missing columns in {table_name}: {missing_cols}")
        return df

    def safe_pass_percent(numerator, denominator):
        """
        Computes pass percentage as float.

        Args:
            numerator (int): Passed row count.
            denominator (int): Total rows.

        Returns:
            float: Pass percent (0-100).
        """
        if denominator == 0:
            return 100.0
        else:
            return round((numerator / denominator) * 100.0, 2)

    def mandatory_fields_check(df):
        """
        Checks for null values in each mandatory field.

        Args:
            df (DataFrame): Data to check, requires all mandatory_fields.

        Returns:
            (str, float): Result ("Passed"/"Failed"), pass percentage.
        """
        total = df.count()
        # Calculate for each row if there is NULL in any of the mandatory fields
        fails = df.withColumn(
            "has_null", sum([col(c).isNull().cast("int") for c in mandatory_fields]) > 0
        )
        pass_count = fails.filter(~col("has_null")).count()
        result = "Passed" if pass_count == total else "Failed"
        return result, safe_pass_percent(pass_count, total)

    def expiry_date_check(df):
        """
        Checks expiry_date > purchase_date for all rows.

        Args:
            df (DataFrame): Data to check, requires expiry_date, purchase_date.

        Returns:
            (str, float): Result ("Passed"/"Failed"), pass percentage.
        """
        total = df.count()
        # expiry_date NULL or purchase_date NULL is considered as Fail
        valid = df.filter(
            (col("expiry_date").isNotNull())
            & (col("purchase_date").isNotNull())
            & (col("expiry_date") > col("purchase_date"))
        )
        pass_count = valid.count()
        result = "Passed" if pass_count == total else "Failed"
        return result, safe_pass_percent(pass_count, total)

    def distinct_value_check(df):
        """
        Checks if (product_ID, batch_number) pairs are distinct.

        Args:
            df (DataFrame): Data to check, requires product_ID, batch_number.

        Returns:
            (str, float): Result ("Passed"/"Failed"), pass percentage.
        """
        pairs = df.groupBy("product_ID", "batch_number").count()
        dupes = pairs.filter(col("count") > 1).count()
        # For pass %, all pairs must be unique
        total_pairs = pairs.count()
        pass_count = total_pairs - dupes
        result = "Passed" if dupes == 0 else "Failed"
        percent = 100.0 if total_pairs == 0 else round(pass_count / total_pairs * 100.0, 2)
        return result, percent

    def data_consistency_check(df):
        """
        Checks quantity > 0 and product_ID starts with P for every row.

        Args:
            df (DataFrame): Data to check, requires quantity, product_ID.

        Returns:
            (str, float): Result ("Passed"/"Failed"), pass percentage.
        """
        total = df.count()
        good = df.filter(
            (col("quantity") > 0) & (col("product_ID").startswith("P"))
        )
        pass_count = good.count()
        result = "Passed" if pass_count == total else "Failed"
        return result, safe_pass_percent(pass_count, total)

    ##############################
    # Main Data Quality Runner   #
    ##############################

    # Load main inventory table for checks; throws exception with missing columns
    all_required_columns = list(set(
        mandatory_fields + expiry_needed + distinct_needed + consistency_needed
    ))

    inventory_df = get_table_with_column_check(inventory_tbl, all_required_columns)

    # 1. Mandatory Fields Check
    mandatory_result, mandatory_percent = mandatory_fields_check(inventory_df)

    # 2. Expiry Date Check
    expiry_result, expiry_percent = expiry_date_check(inventory_df)

    # 3. Distinct Value Check
    distinct_result, distinct_percent = distinct_value_check(inventory_df)

    # 4. Data Consistency Check
    consistency_result, consistency_percent = data_consistency_check(inventory_df)

    #########################
    # Assemble Result Table #
    #########################

    dq_schema = StructType([
        StructField("check_name", StringType(), False),
        StructField("result", StringType(), False),
        StructField("pass_%", DoubleType(), False)
    ])

    dq_results = [
        ("Mandatory Fields Check", mandatory_result, mandatory_percent),
        ("Expiry Date Check", expiry_result, expiry_percent),
        ("Distinct Value Check", distinct_result, distinct_percent),
        ("Data Consistency Check", consistency_result, consistency_percent)
    ]

    dq_results_df = spark.createDataFrame(dq_results, schema=dq_schema)

    # Show the DQ results dataframe
    dq_results_df.show(truncate=False)

except Exception as err:
    # Log any error with a print message (could use dbutils.notebook.exit for handoff if required)
    print(f"ERROR during DQ checks: {err}")
