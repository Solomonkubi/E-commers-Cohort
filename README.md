# E-commers-Cohort
To analyze the purchase behavior of customer cohorts - groups of customers segmented by their first purchase month - in order to understand retention, repeat purchases, and long-term engagement trends.

# Databricks notebook source
--  E-COMMERS SALES COHORT ANALYSIS
--  To analyze the purchase behavior of customer cohorts - groups of customers segmented by their first purchase month - in order to understand retention, repeat purchases, and long-term engagement trends.

-- COMMAND ----------

# Creating Temp view table in file from DBFS

CREATE OR REPLACE TEMPORARY VIEW ecom_orders_view
USING csv
OPTIONS (
  path = "dbfs:/FileStore/tables/ecom_orders.csv",
  header = "true",
  inferSchema = "true"
);

-- COMMAND ----------

select * from ecom_orders_view

-- COMMAND ----------

-- DBTITLE 1,otal sales
select 
  sum(sales), 
  count(distinct customer_id) as total_customers, 
  Count(distinct order_id) as Total_orders 
from ecom_orders_view

-- COMMAND ----------

CREATE OR REPLACE TABLE Cohort_analysis AS (

  WITH cust_purchase AS ( 
    SELECT 
      customer_id, 
      order_date, 
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS order_number 
    FROM ecom_orders_view
  ), 

  ranked_purchases AS ( 
    SELECT 
      customer_id, 
      MAX(CASE WHEN order_number = 1 THEN order_date END) AS first_order_date, 
      MAX(CASE WHEN order_number = 2 THEN order_date END) AS second_order_date 
    FROM cust_purchase 
    GROUP BY customer_id 
  ) 

  SELECT 
    customer_id, 
    first_order_date,
    second_order_date,
    DATE_DIFF(second_order_date, first_order_date) AS days_diff 
  FROM ranked_purchases 
  WHERE first_order_date IS NOT NULL AND second_order_date IS NOT NULL

);



-- COMMAND ----------

SELECT * FROM Cohort_analysis

-- COMMAND ----------

-- retention rate
-- Percentage of customers who placed a second order within the first 3 months

WITH cust_purchase AS (
  SELECT 
    customer_id,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_number
  FROM ecom_orders_view
),

first_second_orders AS (
  SELECT
    customer_id,
    MAX(CASE WHEN order_number = 1 THEN order_date END) AS first_purchase_date,
    MAX(CASE WHEN order_number = 2 THEN order_date END) AS second_purchase_date
  FROM cust_purchase
  GROUP BY customer_id
),

cohort_analysis AS (
  SELECT
    DATE_TRUNC('month', first_purchase_date) AS cohort_month,
    CAST(MONTHS_BETWEEN(second_purchase_date, first_purchase_date) AS INT) AS months_to_second_order,
    customer_id
  FROM first_second_orders
  WHERE first_purchase_date IS NOT NULL AND second_purchase_date IS NOT NULL
)

SELECT
  cohort_month,
  COUNT(*) AS total_customers,
  COUNT(CASE WHEN months_to_second_order = 0 THEN 1 END) AS retained_within_1_month,
  COUNT(CASE WHEN months_to_second_order <= 1 THEN 1 END) AS retained_within_2_months,
  COUNT(CASE WHEN months_to_second_order <= 2 THEN 1 END) AS retained_within_3_months,
  ROUND(COUNT(CASE WHEN months_to_second_order = 0 THEN 1 END) * 1.0 / COUNT(*), 2) AS retention_1_month_pct,
  ROUND(COUNT(CASE WHEN months_to_second_order <= 1 THEN 1 END) * 1.0 / COUNT(*), 2) AS retention_2_month_pct,
  ROUND(COUNT(CASE WHEN months_to_second_order <= 2 THEN 1 END) * 1.0 / COUNT(*), 2) AS retention_3_month_pct
FROM cohort_analysis
GROUP BY cohort_month
ORDER BY cohort_month;


-- COMMAND ----------

-- Repeat Purchase rate by cohort

WITH customer_orders AS (
  SELECT
    customer_id,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_rank,
    COUNT(*) OVER (PARTITION BY customer_id) AS total_orders
  FROM ecom_orders_view
),

first_order_info AS (
  SELECT
    customer_id,
    MIN(order_date) AS first_purchase_date,
    MAX(total_orders) AS total_orders
  FROM customer_orders
  GROUP BY customer_id
),

cohort_customers AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', first_purchase_date) AS cohort_month,
    total_orders
  FROM first_order_info
)

SELECT
  cohort_month,
  COUNT(*) AS total_customers,
  COUNT(CASE WHEN total_orders >= 2 THEN 1 END) AS repeat_2_plus,
  COUNT(CASE WHEN total_orders >= 3 THEN 1 END) AS repeat_3_plus,
  COUNT(CASE WHEN total_orders >= 4 THEN 1 END) AS repeat_4_plus,
  ROUND(COUNT(CASE WHEN total_orders >= 2 THEN 1 END) * 100.0 / COUNT(*), 2) AS repeat_2_pct,
  ROUND(COUNT(CASE WHEN total_orders >= 3 THEN 1 END) * 100.0 / COUNT(*), 2) AS repeat_3_pct,
  ROUND(COUNT(CASE WHEN total_orders >= 4 THEN 1 END) * 100.0 / COUNT(*), 2) AS repeat_4_pct
FROM cohort_customers
GROUP BY cohort_month
ORDER BY cohort_month;


-- COMMAND ----------

 --Cohort size by month:
-- To get each customer's first purchase date

WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_purchase_date
    FROM ecom_orders_view
    GROUP BY customer_id
)

-- Grouping customers by month of first purchase
SELECT 
    DATE_TRUNC('month', first_purchase_date) AS cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size
FROM first_orders
GROUP BY DATE_TRUNC('month', first_purchase_date)
ORDER BY cohort_month;

--- to see the dashboard of the analysis
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2815371933513622/2048459982529056/375578256465411/latest.html
