-- Databricks SQL script
-- Purpose: Create a data quality validation report for the drug_inventory_management table
-- Author: Khanh Trinh
-- Date: 2025-06-24
-- Description: This script performs data quality checks on the drug_inventory_management table based on provided rules and generates a report with the check name, result, and pass percentage.

-- Common Table Expression (CTE) for each of the data quality checks
WITH MandatoryFieldsCheck AS (
  -- Check to ensure mandatory fields are not null
  SELECT
    'Mandatory Fields Check' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END AS result,
    CASE WHEN COUNT(*) = 0 THEN 100.0 ELSE 100.0 * (SELECT COUNT(*) FROM purgo_playground.drug_inventory_management) / COUNT(*) END AS pass_percentage
  FROM purgo_playground.drug_inventory_management
  WHERE product_ID IS NULL OR product_name IS NULL OR quantity IS NULL OR location IS NULL OR expiry_date IS NULL OR 
        batch_number IS NULL OR supplier_ID IS NULL       
),
ExpiryDateCheck AS (
  -- Check to ensure expiry_date is greater than purchase_date
  SELECT
    'Expiry Date Check' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END AS result,
    CASE WHEN COUNT(*) = 0 THEN 100.0 ELSE 100.0 * (SELECT COUNT(*) FROM purgo_playground.drug_inventory_management) / COUNT(*) END AS pass_percentage
  FROM purgo_playground.drug_inventory_management
  WHERE expiry_date <= purchase_date
),
DistinctValueCheck AS (
  -- Check to ensure product_ID and batch_number are distinct
  SELECT
    'Distinct Value Check' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END AS result,
    CASE WHEN COUNT(*) = 0 THEN 100.0 ELSE 100.0 * 2 * (SELECT COUNT(*) FROM purgo_playground.drug_inventory_management) / (SELECT COUNT(DISTINCT product_ID) + COUNT(DISTINCT batch_number) FROM purgo_playground.drug_inventory_management) END AS pass_percentage
  FROM (
    SELECT product_ID, batch_number FROM purgo_playground.drug_inventory_management
    GROUP BY product_ID, batch_number HAVING COUNT(*) > 1
  )
),
DataConsistencyCheck AS (
  -- Check to ensure quantity is positive and product_ID starts with "P"
  SELECT
    'Data Consistency Check' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END AS result,
    CASE WHEN COUNT(*) = 0 THEN 100.0 ELSE 100.0 * (SELECT COUNT(*) FROM purgo_playground.drug_inventory_management) / COUNT(*) END AS pass_percentage
  FROM purgo_playground.drug_inventory_management
  WHERE quantity <= 0 OR NOT product_ID LIKE 'P%'
)

-- Final data quality report combining results from all checks
SELECT * FROM MandatoryFieldsCheck
UNION ALL
SELECT * FROM ExpiryDateCheck
UNION ALL
SELECT * FROM DistinctValueCheck
UNION ALL
SELECT * FROM DataConsistencyCheck;
