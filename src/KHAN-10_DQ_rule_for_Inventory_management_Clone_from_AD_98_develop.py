spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script - Data Quality Validation for Inventory Management
# Purpose: Perform data quality checks on 'purgo_playground.drug_inventory_management' as per DQ_rules_IM
# Author: Khanh Trinh
# Date: 2025-06-25
# Description: This script validates key DQ rules (mandatory fields, expiry date check, distinct value, data consistency) over the inventory table, enforcing schema and reporting a summary dataframe of rule results ('check_name', 'result', 'pass_').

# from pyspark.sql import SparkSession  # SparkSession pre-initialized in Databricks

from pyspark.sql.functions import col, count, when, isnan, lit, regexp_extract, expr, monotonically_increasing_id, sum as spark_sum, countDistinct, round  
from pyspark.sql.types import StringType, LongType, TimestampType, DoubleType, StructType, StructField  

# ========= CONFIGURATION =========

CATALOG = "purgo_databricks"
SCHEMA = "purgo_playground"
TABLE = "drug_inventory_management"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

REQUIRED_COLUMNS = [
    "product_ID", "product_name", "quantity", "location",
    "expiry_date", "batch_number", "supplier_ID", "purchase_date"
]

DQ_RULES = [
    {"check_name": "Mandatory Fields Check"},
    {"check_name": "Expiry Date Check"},
    {"check_name": "Distinct value Check"},
    {"check_name": "Data Consistency Check"}
]

# ========= UTILITY FUNCTIONS =========

def check_table_exists(catalog:str, schema:str, table:str) -> bool:
    """
    Checks if a table exists in the Unity Catalog.
    Args:
        catalog (str): Catalog name.
        schema (str): Schema/database name.
        table (str): Table name.

    Returns:
        bool: True if table exists.
    """
    try:
        tables = spark.catalog.listTables(f"{catalog}.{schema}")
        for t in tables:
            if t.name == table and t.tableType in ["MANAGED", "EXTERNAL"]:
                return True
        return False
    except Exception:
        return False

def enforce_columns(df, required_cols:list) -> list:
    """
    Checks if all required columns exist in a DataFrame.
    Args:
        df (DataFrame): Input dataframe.
        required_cols (list): Required column names.

    Returns:
        list: List of missing column names (empty if none missing).
    """
    existing = set(df.columns)
    missing = [col_ for col_ in required_cols if col_ not in existing]
    return missing

def safe_load_table(full_table:str):
    """
    Attempts to load a table as DataFrame, raises descriptive error if not present.
    Args:
        full_table (str): Full Unity Catalog table path.

    Returns:
        DataFrame: Table as DataFrame.
    """
    try:
        return spark.table(full_table)
    except Exception:
        raise RuntimeError(f"Table {full_table} does not exist")

def result_row(name:str, passed:bool, pct:float) -> dict:
    """
    Formats a DQ result as a dict.
    Args:
        name (str): Check name.
        passed (bool): Pass/fail.
        pct (float): Pass percent.

    Returns:
        dict: Row dict.
    """
    return {
        "check_name": name,
        "result": "pass" if passed else "fail",
        "pass_": round(pct, 2)
    }

# ========= MAIN DATA QUALITY LOGIC =========

def dq_check_inventory_management():
    """
    Runs all the required data quality checks and returns their overall status.
    Returns:
        DataFrame: DataFrame with columns check_name, result, pass_
    """
    # Table existence check
    if not check_table_exists(CATALOG, SCHEMA, TABLE):
        raise RuntimeError(f"Table {SCHEMA}.{TABLE} does not exist")

    # Load table
    df = safe_load_table(FULL_TABLE)

    # Schema/column enforcement
    missing_cols = enforce_columns(df, REQUIRED_COLUMNS)
    if missing_cols:
        raise RuntimeError(f"Missing required column(s) for data quality rules: {', '.join(missing_cols)}")

    total_rows = df.count() if not df.rdd.isEmpty() else 0
    dq_results = []

    # --- Mandatory Fields Check ---
    # All required columns not NULL
    if total_rows == 0:
        man_pass_pct = 100.0
        man_passed = True
    else:
        not_null_cond = (
            (col('product_ID').isNotNull()) &
            (col('product_name').isNotNull()) &
            (col('quantity').isNotNull()) &
            (col('location').isNotNull()) &
            (col('expiry_date').isNotNull()) &
            (col('batch_number').isNotNull()) &
            (col('supplier_ID').isNotNull())
        )
        man_valid = df.filter(not_null_cond).count()
        man_pass_pct = (man_valid / total_rows) * 100 if total_rows > 0 else 100.0
        man_passed = (man_valid == total_rows)
    dq_results.append(result_row("Mandatory Fields Check", man_passed, man_pass_pct))

    # --- Expiry Date Check ---
    # expiry_date > purchase_date, no nulls
    if total_rows == 0:
        exp_pass_pct = 100.0
        exp_passed = True
    else:
        expiry_valid = df.filter(
            col('expiry_date').isNotNull() &
            col('purchase_date').isNotNull() &
            (col('expiry_date') > col('purchase_date'))
        ).count()
        exp_pass_pct = (expiry_valid / total_rows) * 100 if total_rows > 0 else 100.0
        exp_passed = (expiry_valid == total_rows)
    dq_results.append(result_row("Expiry Date Check", exp_passed, exp_pass_pct))

    # --- Distinct value Check ---
    # (product_ID, batch_number) all distinct
    if total_rows == 0:
        uniq_pass_pct = 100.0
        uniq_passed = True
    else:
        num_distinct_pairs = df.select('product_ID', 'batch_number').distinct().count()
        uniq_pass_pct = (num_distinct_pairs / total_rows) * 100 if total_rows > 0 else 100.0
        uniq_passed = (num_distinct_pairs == total_rows)
    dq_results.append(result_row("Distinct value Check", uniq_passed, uniq_pass_pct))

    # --- Data Consistency Check ---
    # quantity > 0 and product_ID starts with 'P'
    if total_rows == 0:
        dc_pass_pct = 100.0
        dc_passed = True
    else:
        dc_cond = (
            (col('quantity').isNotNull()) &
            (col('quantity') > 0) &
            (col('product_ID').isNotNull()) &
            (col('product_ID').rlike('^P.*'))
        )
        dc_valid = df.filter(dc_cond).count()
        dc_pass_pct = (dc_valid / total_rows) * 100 if total_rows > 0 else 100.0
        dc_passed = (dc_valid == total_rows)
    dq_results.append(result_row("Data Consistency Check", dc_passed, dc_pass_pct))

    # Format result to DataFrame/schema
    schema = StructType([
        StructField("check_name", StringType(), False),
        StructField("result", StringType(), False),
        StructField("pass_", DoubleType(), False)
    ])
    dq_df = spark.createDataFrame(dq_results, schema=schema)
    return dq_df

# ========== EXECUTE MAIN ROUTINE ==========

# The following block will execute the DQ suite and print result table to stdout.
try:
    dq_result_df = dq_check_inventory_management()
    dq_result_df.show(truncate=False)
except RuntimeError as e:
    print(f"Data Quality validation FAILED: {str(e)}")

# (Do not call spark.stop() -- Databricks notebook sessions expect Spark alive!)
