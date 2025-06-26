%pip install pytest

spark.catalog.setCurrentCatalog("purgo_databricks")


# PySpark script - Data Quality Framework for Inventory Management Table
# Purpose: Data quality rules validation for purgo_playground.drug_inventory_management
# Author: Khanh Trinh
# Date: 2025-06-25
# Description: This script checks the key data quality rules as defined in DQ_rules_IM sheet for the drug_inventory_management table.
# It reports pass/fail and pass_% per rule: Mandatory Fields, Expiry Date, Distinct Value, and Data Consistency.
# The script enforces input schema, checks table/column existence, and outputs a summary dataframe of DQ results.

# from pyspark.sql import SparkSession                   # SparkSession is pre-initialized in Databricks
from pyspark.sql.functions import col, count, when, isnan, lit, regexp_extract, expr, monotonically_increasing_id, sum as spark_sum, countDistinct, round  
from pyspark.sql.types import StringType, LongType, TimestampType, DoubleType, StructType, StructField  
import pytest  

# ========== CONFIGURATION ==========

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

# ========== UTILITY FUNCTIONS ==========

def check_table_exists(catalog:str, schema:str, table:str) -> bool:
    """
    Checks if the table exists in the provided Unity Catalog location.
    Args:
        catalog (str): Unity Catalog name.
        schema (str): Schema/database name.
        table (str): Table name.

    Returns:
        bool: True if table exists, False otherwise.
    """
    try:
        tables = spark.catalog.listTables(f"{catalog}.{schema}")
        for t in tables:
            if t.name == table and t.tableType in ["MANAGED", "EXTERNAL"]:
                return True
        return False
    except Exception as e:
        return False

def enforce_columns(df, required_cols:list) -> list:
    """
    Checks if all required columns exist in a DataFrame.
    Args:
        df (DataFrame): Input dataframe.
        required_cols (list): List of required column names.

    Returns:
        list: List of missing column names (empty if none missing).
    """
    existing = set(df.columns)
    missing = [col for col in required_cols if col not in existing]
    return missing

def safe_load_table(full_table:str):
    """
    Attempts to load a table as a DataFrame.
    Args:
        full_table (str): Full Unity Catalog table path.

    Returns:
        DataFrame: DataFrame loaded from table.
    """
    try:
        return spark.table(full_table)
    except Exception as e:
        raise RuntimeError(f"Table {full_table} does not exist")

def result_row(name:str, passed:bool, pct:float) -> dict:
    """
    Formats a DQ result as a dict.
    Args:
        name (str): DQ check name.
        passed (bool): Pass status.
        pct (float): Pass percentage.

    Returns:
        dict: Row dict.
    """
    return {
        "check_name": name,
        "result": "pass" if passed else "fail",
        "pass_": round(pct, 2)
    }

# ========== MAIN DATA QUALITY LOGIC ==========

def dq_check_inventory_management():
    """
    Performs DQ checks on the drug_inventory_management table and returns a summary DataFrame.

    Returns:
        DataFrame: DataFrame with columns check_name, result, pass_
    """
    # Table existence check
    if not check_table_exists(CATALOG, SCHEMA, TABLE):
        raise RuntimeError(f"Table {SCHEMA}.{TABLE} does not exist")

    # Load the data
    df = safe_load_table(FULL_TABLE)

    # Schema/column checks
    missing_cols = enforce_columns(df, REQUIRED_COLUMNS)
    if missing_cols:
        raise RuntimeError(f"Missing required column(s) for data quality rules: {', '.join(missing_cols)}")

    total_rows = df.count() if df.rdd.isEmpty() is False else 0
    dq_results = []

    # ========== 1. Mandatory Fields Check ==========
    # All: product_ID, product_name, quantity, location, expiry_date, batch_number, supplier_ID NOT NULL
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

    # ========== 2. Expiry Date Check ==========
    # expiry_date > purchase_date (both non-null)
    # Null in either is invalid
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

    # ========== 3. Distinct value Check ==========
    # Unique (product_ID, batch_number) pairing
    if total_rows == 0:
        uniq_pass_pct = 100.0
        uniq_passed = True
    else:
        num_distinct_pairs = df.select('product_ID', 'batch_number').distinct().count()
        uniq_pass_pct = (num_distinct_pairs / total_rows) * 100 if total_rows > 0 else 100.0
        uniq_passed = (num_distinct_pairs == total_rows)
    dq_results.append(result_row("Distinct value Check", uniq_passed, uniq_pass_pct))

    # ========== 4. Data Consistency Check ==========
    # quantity > 0 and product_ID startswith "P"
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

    # Format results as DataFrame
    schema = StructType([
        StructField("check_name", StringType(), False),
        StructField("result", StringType(), False),
        StructField("pass_", DoubleType(), False)
    ])
    dq_df = spark.createDataFrame(dq_results, schema=schema)
    return dq_df

# ========== TEST SUITE: UNIT, INTEGRATION & SCHEMA VALIDATION ==========

def create_test_inventory_df(data: list) -> "DataFrame":
    """
    Creates a test DataFrame matching the drug_inventory_management schema for test scenarios.
    Args:
        data (list): list of dicts with row data.

    Returns:
        DataFrame: test DataFrame.
    """
    schema = StructType([
        StructField("product_ID", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("location", StringType(), True),
        StructField("expiry_date", TimestampType(), True),
        StructField("batch_number", StringType(), True),
        StructField("supplier_ID", StringType(), True),
        StructField("purchase_date", TimestampType(), True),
        StructField("last_restocked_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("data_loaded_at", TimestampType(), True)
    ])
    return spark.createDataFrame(data, schema=schema)

def assert_dq_result(df, check_name, expected_result, expected_pct, tolerance=0.01):
    """
    Asserts a DQ result row by check_name.
    Args:
        df (DataFrame): Result DQ summary dataframe.
        check_name (str): Check name.
        expected_result (str): Expected result ("pass"/"fail").
        expected_pct (float): Expected pass %.
        tolerance (float): allowed delta for percent.

    Returns:
        None. Raises AssertionError if fails.
    """
    row = df.filter(col("check_name") == check_name).collect()
    assert row, f"Check {check_name} not in results"
    res = row[0]
    assert res['result'] == expected_result, f"{check_name} expected result {expected_result}, got {res['result']}"
    assert abs(res['pass_'] - expected_pct) <= tolerance, f"{check_name} pass_ {res['pass_']} != {expected_pct}"

def inject_inventory_test_table(test_df, drop_if_exists=True):
    """
    Replace purgo_playground.drug_inventory_management by test_df, for isolated DQ test runs.
    Args:
        test_df (DataFrame): Test DataFrame to use as table.
        drop_if_exists (bool): Drop the table if exists.

    Returns:
        None.
    """
    if drop_if_exists and check_table_exists(CATALOG, SCHEMA, TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")
    test_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FULL_TABLE)

def cleanup_inventory_table():
    """
    Drops the inventory test table.
    Returns:
        None.
    """
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

# ====================
# UNIT TESTS PER RULE
# ====================

def test_schema_validation_and_output_structure():
    """
    Test: Output schema must match (check_name:string, result:string, pass_:double)
    """
    # minimal valid data
    test_data = [
        {
            "product_ID": "PTEST1", "product_name": "X", "quantity": 1, "location": "L", "expiry_date": None,
            "batch_number": "B", "supplier_ID": "S", "purchase_date": None, "last_restocked_date": None, "status": "A", "data_loaded_at": None
        }
    ]
    test_df = create_test_inventory_df(test_data)
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    output_fields = set([f.name for f in dq_df.schema.fields])
    assert output_fields == {"check_name", "result", "pass_"}
    for row in dq_df.collect():
        assert row["check_name"] in [
            "Mandatory Fields Check", "Expiry Date Check", "Distinct value Check", "Data Consistency Check"
        ]
        assert row["result"] in ["pass", "fail"]
        assert 0.0 <= row["pass_"] <= 100.0
    cleanup_inventory_table()

def test_table_not_exists():
    """
    Test: Error raised if table missing.
    """
    cleanup_inventory_table()
    try:
        dq_check_inventory_management()
        assert False, "Should raise error if table missing"
    except RuntimeError as e:
        assert "does not exist" in str(e)

def test_missing_columns_error():
    """
    Test: Error for missing columns.
    """
    test_data = [
        {"product_ID": "P1001", "product_name": "A"}
    ]
    wrong_schema = StructType([
        StructField("product_ID", StringType(), True),
        StructField("product_name", StringType(), True)
    ])
    df = spark.createDataFrame(test_data, schema=wrong_schema)
    inject_inventory_test_table(df)
    try:
        dq_check_inventory_management()
        assert False, "Should raise error if columns missing"
    except RuntimeError as e:
        assert "Missing required column(s)" in str(e)
    cleanup_inventory_table()

def test_mandatory_fields_check_all_nulls():
    """
    Test: Fails if any mandatory required field is null.
    """
    from datetime import datetime
    base = dict(product_ID="P12", product_name="Paracet", quantity=5, location="NY", expiry_date=datetime(2026,1,1), batch_number="B1", supplier_ID="S1", purchase_date=datetime(2025,1,1), last_restocked_date=None, status="active", data_loaded_at=None)
    for colname in REQUIRED_COLUMNS:
        row = base.copy()
        row[colname] = None
        test_df = create_test_inventory_df([row])
        inject_inventory_test_table(test_df)
        dq_df = dq_check_inventory_management()
        assert_dq_result(dq_df, "Mandatory Fields Check", "fail", 0.0)
        cleanup_inventory_table()

def test_mandatory_fields_check_pass():
    """
    Test: All mandatory fields, not null, should pass
    """
    from datetime import datetime
    test_df = create_test_inventory_df([
        {
            "product_ID":"P1111", "product_name":"DrugA", "quantity":22, "location":"LON",
            "expiry_date":datetime(2025,1,1), "batch_number":"BATX31", "supplier_ID":"SUP1",
            "purchase_date":datetime(2024,1,1), "last_restocked_date":None, "status":"active", "data_loaded_at":None
        }
    ])
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    assert_dq_result(dq_df, "Mandatory Fields Check", "pass", 100.0)
    cleanup_inventory_table()

def test_expiry_date_check_various():
    """
    Test: expiry_date > purchase_date rule
    """
    from datetime import datetime
    rows = [
        # expiry <= purchase = fail
        {"product_ID":"P1", "product_name":"A", "quantity":1, "location":"X", "expiry_date":datetime(2022,1,1), "batch_number":"A", "supplier_ID":"S", "purchase_date":datetime(2023,1,1), "last_restocked_date":None, "status":"a", "data_loaded_at":None},
        # expiry == purchase = fail
        {"product_ID":"P2", "product_name":"B", "quantity":1, "location":"X", "expiry_date":datetime(2025,1,1), "batch_number":"B", "supplier_ID":"S", "purchase_date":datetime(2025,1,1), "last_restocked_date":None, "status":"a", "data_loaded_at":None},
        # expiry > purchase = pass
        {"product_ID":"P3", "product_name":"C", "quantity":1, "location":"X", "expiry_date":datetime(2026,1,1), "batch_number":"C", "supplier_ID":"S", "purchase_date":datetime(2025,1,1), "last_restocked_date":None, "status":"a", "data_loaded_at":None}
    ]
    test_df = create_test_inventory_df(rows)
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    assert_dq_result(dq_df, "Expiry Date Check", "fail", (1/3)*100)
    cleanup_inventory_table()

def test_distinct_value_check_scenarios():
    """
    Test: (product_ID, batch_number) must be unique
    """
    from datetime import datetime
    rows = [
        # dupe combo
        {"product_ID":"P10", "product_name":"a", "quantity":1, "location":"a", "expiry_date":datetime(2025,1,1), "batch_number":"Z1", "supplier_ID":"x", "purchase_date":datetime(2024,1,1), "last_restocked_date":None, "status":"ss", "data_loaded_at":None},
        {"product_ID":"P10", "product_name":"b", "quantity":2, "location":"b", "expiry_date":datetime(2025,1,2), "batch_number":"Z1", "supplier_ID":"x", "purchase_date":datetime(2024,1,1), "last_restocked_date":None, "status":"ss", "data_loaded_at":None},
        {"product_ID":"P21", "product_name":"c", "quantity":3, "location":"c", "expiry_date":datetime(2025,1,3), "batch_number":"Z2", "supplier_ID":"y", "purchase_date":datetime(2024,1,1), "last_restocked_date":None, "status":"tt", "data_loaded_at":None}
    ]
    test_df = create_test_inventory_df(rows)
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    # 2 distinct pairs out of 3 rows = 66.67%
    assert_dq_result(dq_df, "Distinct value Check", "fail", 66.67, tolerance=0.02)
    cleanup_inventory_table()

def test_data_consistency_check_quantity_and_pattern():
    """
    Test: quantity positive and product_ID pattern
    """
    from datetime import datetime
    # fail: quantity null or <=0 or product_ID not start with P
    samples = [
        # pass
        {"product_ID":"P111", "product_name":"D", "quantity":10, "location":"A", "expiry_date":datetime(2025,6,1), "batch_number":"B1", "supplier_ID":"Z", "purchase_date":datetime(2024,1,1),"last_restocked_date":None,"status":"done","data_loaded_at":None},
        # fail: negative quantity
        {"product_ID":"P112", "product_name":"E", "quantity":-2, "location":"B", "expiry_date":datetime(2025,6,2), "batch_number":"B2", "supplier_ID":"Y", "purchase_date":datetime(2024,1,1), "last_restocked_date":None,"status":"done","data_loaded_at":None},
        # fail: product_ID not starting with P
        {"product_ID":"X113", "product_name":"F", "quantity":9, "location":"C", "expiry_date":datetime(2025,6,3), "batch_number":"B3", "supplier_ID":"X", "purchase_date":datetime(2024,1,1), "last_restocked_date":None,"status":"done","data_loaded_at":None}
    ]
    test_df = create_test_inventory_df(samples)
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    # Only first row is valid: 1/3 = 33.33%
    assert_dq_result(dq_df, "Data Consistency Check", "fail", 33.33, tolerance=0.02)
    cleanup_inventory_table()

def test_combined_failures():
    """
    Test: Multiple rules fail on mixed quality data
    """
    from datetime import datetime
    rows = [
        # Row 1: fail mandatory (product_ID null), pass others
        {"product_ID": None, "product_name":"A", "quantity":1, "location":"Z", "expiry_date":datetime(2025,1,1), "batch_number":"B7", "supplier_ID":"X", "purchase_date":datetime(2024,1,1), "last_restocked_date":None, "status":"good", "data_loaded_at":None},
        # Row 2: pass mandatory, fail expiry_date/purchase_date, fail data consistency (quantity 0), dup pair
        {"product_ID":"P222", "product_name":"B", "quantity":0, "location":"Y", "expiry_date":datetime(2023,1,1),"batch_number":"B7",
         "supplier_ID":"X", "purchase_date":datetime(2024,1,1), "last_restocked_date":None, "status":"bad","data_loaded_at":None}
    ]
    test_df = create_test_inventory_df(rows)
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    row_dict = {r['check_name']:r for r in dq_df.collect()}
    # Only second row is valid on mandatory, so 50%. For expiry (expiry_date < purchase_date) both not passing = 0%. For distinct, both pairs same, so 1/2=50%. Only first row passes data consistency (quantity>0 and "P*"), but actually quantity is 0 row 2 (should fail), and product_ID null row 1 (should fail), so both rows fail: 0/2.
    assert_dq_result(dq_df, "Mandatory Fields Check", "fail", 50.0)
    assert_dq_result(dq_df, "Expiry Date Check", "fail", 0.0)
    assert_dq_result(dq_df, "Distinct value Check", "fail", 50.0)
    assert_dq_result(dq_df, "Data Consistency Check", "fail", 0.0)
    cleanup_inventory_table()

def test_empty_table_passes():
    """
    Test: If no rows, all rules should pass with 100%
    """
    test_df = create_test_inventory_df([])
    inject_inventory_test_table(test_df)
    dq_df = dq_check_inventory_management()
    for r in dq_df.collect():
        assert r["result"] == "pass"
        assert r["pass_"] == 100.0
    cleanup_inventory_table()

# ========== RUN DATA QUALITY CHECK MAIN (for interactive, not for test runs) ==========

try:
    dq_result_df = dq_check_inventory_management()
    dq_result_df.show(truncate=False)
except RuntimeError as e:
    print(f"Data Quality validation FAILED: {str(e)}")

# (No spark.stop()! Might break Databricks notebook cluster)
