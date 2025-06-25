spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for data quality checks on purgo_playground.drug_inventory_management table
# Purpose: Implement and aggregate data quality rules as described by DQ_rules_IM for inventory data
# Author: Khanh Trinh
# Date: 2025-06-25
# Description: This script loads inventory data and checks for: non-null mandatory fields, valid expiry dates, uniqueness of (product_ID, batch_number), and field/value consistency. It summarizes pass/fail, and per-check pass % for reporting and alerting integration.

# Imports for PySpark transformations and types
from pyspark.sql.functions import col, when, count, sum as spark_sum, lit, expr  
from pyspark.sql.types import StringType, DoubleType, StructType, StructField   

# Read source: purgo_playground.drug_inventory_management with expected schema
drug_inv_df = spark.table("purgo_playground.drug_inventory_management")

# =======================
# Data Quality Checks
# =======================

def mandatory_fields_check(df):
    """
    Check for null values in mandatory fields.

    Args:
        df (pyspark.sql.DataFrame): Drug inventory dataframe

    Returns:
        Tuple[int, int]: (Number of passing rows, Total rows)
    """
    mandatory_fields = ["product_ID", "product_name", "quantity", "location", "expiry_date", "batch_number", "supplier_ID"]
    # Row is passing if all mandatory fields are not null
    passing_expr = " AND ".join([f"{f} IS NOT NULL" for f in mandatory_fields])
    pass_count = df.filter(expr(passing_expr)).count()
    total_count = df.count()
    return pass_count, total_count

def expiry_date_check(df):
    """
    Ensure expiry_date is greater than purchase_date.

    Args:
        df (pyspark.sql.DataFrame): Drug inventory dataframe

    Returns:
        Tuple[int, int]: (Number of passing rows, Total rows)
    """
    # Nulls or equal dates considered failures
    pass_count = df.filter((col("expiry_date").isNotNull()) & (col("purchase_date").isNotNull()) & (col("expiry_date") > col("purchase_date"))).count()
    total_count = df.filter((col("expiry_date").isNotNull()) & (col("purchase_date").isNotNull())).count()
    return pass_count, total_count

def distinct_value_check(df):
    """
    Check that (product_ID, batch_number) pairs are unique.

    Args:
        df (pyspark.sql.DataFrame): Drug inventory dataframe

    Returns:
        Tuple[int, int]: (Number of passing rows, Total rows)
    """
    # Count number of repeated (product_ID, batch_number) pairs
    grouped = df.groupBy("product_ID", "batch_number").count()
    # Count pairs with duplicates (count > 1)
    duplicate_pairs = grouped.filter(col("count") > 1)
    if duplicate_pairs.count() == 0:
        # All pairs are unique
        return df.count(), df.count()
    else:
        # Rows violating uniqueness
        violating_keys = duplicate_pairs.select("product_ID", "batch_number")
        # Join back to original DataFrame to count violating rows
        join_cond = [df.product_ID == violating_keys.product_ID, df.batch_number == violating_keys.batch_number]
        violating_rows = df.join(violating_keys, on=join_cond, how="inner").count()
        return df.count() - violating_rows, df.count()

def data_consistency_check(df):
    """
    Ensure quantity > 0 and product_ID starts with 'P'.

    Args:
        df (pyspark.sql.DataFrame): Drug inventory dataframe

    Returns:
        Tuple[int, int]: (Number of passing rows, Total rows)
    """
    passing = df.filter((col("quantity") > 0) & (col("product_ID").startswith("P")))
    return passing.count(), df.count()

# =======================
# Run All Checks & Assemble Output DataFrame
# =======================

# For each check, calculate count of pass and total, and derive result and pass %
pass_mand, total_mand = mandatory_fields_check(drug_inv_df)
res_mand = "Passed" if (total_mand == 0 or pass_mand == total_mand) else "Failed"
perc_mand = float(pass_mand) / total_mand * 100 if total_mand else 100.0     # 100% if empty table

pass_expiry, total_expiry = expiry_date_check(drug_inv_df)
res_expiry = "Passed" if (total_expiry == 0 or pass_expiry == total_expiry) else "Failed"
perc_expiry = float(pass_expiry) / total_expiry * 100 if total_expiry else 100.0

pass_dist, total_dist = distinct_value_check(drug_inv_df)
res_dist = "Passed" if (total_dist == 0 or pass_dist == total_dist) else "Failed"
perc_dist = float(pass_dist) / total_dist * 100 if total_dist else 100.0

pass_cons, total_cons = data_consistency_check(drug_inv_df)
res_cons = "Passed" if (total_cons == 0 or pass_cons == total_cons) else "Failed"
perc_cons = float(pass_cons) / total_cons * 100 if total_cons else 100.0

# Prepare the final DQ results DataFrame as specified
dq_results_schema = StructType([
    StructField("check_name", StringType(), nullable=False),
    StructField("result", StringType(), nullable=False),
    StructField("pass_%", DoubleType(), nullable=False)
])

dq_results_data = [
    ("Mandatory Fields Check", res_mand, perc_mand),
    ("Expiry Date Check", res_expiry, perc_expiry),
    ("Distinct Value Check", res_dist, perc_dist),
    ("Data Consistency Check", res_cons, perc_cons)
]

dq_results_df = spark.createDataFrame(dq_results_data, schema=dq_results_schema)

# Show the summarized results
dq_results_df.show(truncate=False)

# End of script -- results are in dq_results_df
