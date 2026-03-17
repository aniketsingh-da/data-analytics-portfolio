-- =========================================================
-- US Pharma Sales Pipeline
-- Layer : LDG → STG → FACT → INT → MDM → BUS → RPT
-- =========================================================

-- ========================
-- 1. LDG Layer – Raw Capture
-- ========================

CREATE TABLE pharma_sales_ldg.raw_sales (
    patient_id     VARCHAR(50) NOT NULL,
    drug_name      VARCHAR(100) NOT NULL,
    trx            DECIMAL(10,2) DEFAULT 0,
    nrx            DECIMAL(10,2) DEFAULT 0,
    cbnrx          DECIMAL(10,2) DEFAULT 0,
    sales_value    DECIMAL(12,2) DEFAULT 0,
    txn_date       DATE NOT NULL,
    npi            VARCHAR(20),
    region         VARCHAR(50),
    PRIMARY KEY (patient_id, txn_date, drug_name)
);

-- Index for faster filtering by date and drug
CREATE INDEX idx_raw_sales_date_drug ON pharma_sales_ldg.raw_sales(txn_date, drug_name);

-- Daily aggregation
CREATE TABLE pharma_sales_ldg.daily_agg AS
SELECT txn_date, drug_name, region,
       SUM(trx) AS total_trx,
       SUM(nrx) AS total_nrx,
       SUM(cbnrx) AS total_cbnrx,
       SUM(sales_value) AS total_sales
FROM pharma_sales_ldg.raw_sales
GROUP BY txn_date, drug_name, region;

-- =========================================================
-- 2️ STG Layer – Cleaning, QC & Master Data
-- =========================================================

WITH cleaned AS (
    SELECT 
        TRIM(UPPER(patient_id)) AS patient_id,
        TRIM(UPPER(drug_name)) AS drug_name,
        CAST(trx AS DECIMAL(10,2)) AS trx,
        CAST(nrx AS DECIMAL(10,2)) AS nrx,
        CAST(cbnrx AS DECIMAL(10,2)) AS cbnrx,
        CAST(sales_value AS DECIMAL(12,2)) AS sales_value,
        CAST(txn_date AS DATE) AS txn_date,
        TRIM(UPPER(npi)) AS npi,
        TRIM(UPPER(region)) AS region
    FROM pharma_sales_ldg.raw_sales
)
SELECT * INTO pharma_sales_stg.clean_sales FROM cleaned;

-- ========================
-- QC Checks (6)
-- ========================

-- QC 1: Null check
CREATE TABLE pharma_sales_stg.qc_nulls AS
SELECT field,
       COUNT(*) AS null_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_null
FROM (
    SELECT 'patient_id' AS field, patient_id FROM pharma_sales_stg.clean_sales
    UNION ALL
    SELECT 'drug_name', drug_name FROM pharma_sales_stg.clean_sales
    UNION ALL
    SELECT 'sales_value', sales_value FROM pharma_sales_stg.clean_sales
    UNION ALL
    SELECT 'region', region FROM pharma_sales_stg.clean_sales
    UNION ALL
    SELECT 'txn_date', txn_date FROM pharma_sales_stg.clean_sales
) t
WHERE t.field IS NULL
GROUP BY field;

-- QC 2: Duplicate check
CREATE TABLE pharma_sales_stg.qc_duplicates AS
SELECT patient_id, txn_date, drug_name,
       COUNT(*) AS dup_count
FROM pharma_sales_stg.clean_sales
GROUP BY patient_id, txn_date, drug_name
HAVING COUNT(*) > 1;

-- QC 3: Negative sales
CREATE TABLE pharma_sales_stg.qc_negatives AS
SELECT *
FROM pharma_sales_stg.clean_sales
WHERE trx < 0 OR nrx < 0 OR cbnrx < 0 OR sales_value < 0;

-- QC 4: Outlier detection (Z-score)
CREATE TABLE pharma_sales_stg.qc_outliers AS
SELECT *,
       (sales_value - AVG(sales_value) OVER (PARTITION BY drug_name)) / 
       NULLIF(STDDEV(sales_value) OVER (PARTITION BY drug_name),0) AS z_score
FROM pharma_sales_stg.clean_sales
WHERE ABS(
       (sales_value - AVG(sales_value) OVER (PARTITION BY drug_name)) / 
       NULLIF(STDDEV(sales_value) OVER (PARTITION BY drug_name),0)
) > 3;

-- QC 5: Missing NPI
CREATE TABLE pharma_sales_stg.qc_missing_npi AS
SELECT *
FROM pharma_sales_stg.clean_sales
WHERE npi IS NULL;

-- QC 6: Region coverage check
CREATE TABLE pharma_sales_stg.qc_region_coverage AS
SELECT region, COUNT(*) AS records
FROM pharma_sales_stg.clean_sales
GROUP BY region
HAVING COUNT(*) < 10; -- very low coverage

-- =========================================================
-- 3️ FACT Layer – Aggregations
-- =========================================================

-- CTE: Monthly aggregation
WITH monthly AS (
    SELECT DATE_TRUNC('month', txn_date) AS month,
           drug_name, region,
           SUM(trx) AS total_trx,
           SUM(nrx) AS total_nrx,
           SUM(cbnrx) AS total_cbnrx,
           SUM(sales_value) AS total_sales
    FROM pharma_sales_stg.clean_sales
    GROUP BY 1,2,3
)
SELECT * INTO pharma_sales_fact.monthly_fact
FROM monthly;

-- Provider-level aggregation with ranking
CREATE TABLE pharma_sales_fact.provider_fact AS
SELECT npi, drug_name, region,
       SUM(trx) AS provider_trx,
       SUM(nrx) AS provider_nrx,
       SUM(cbnrx) AS provider_cbnrx,
       RANK() OVER (PARTITION BY region, drug_name ORDER BY SUM(trx) DESC) AS rank_trx,
       ROW_NUMBER() OVER (PARTITION BY region, drug_name ORDER BY SUM(sales_value) DESC) AS rank_sales
FROM pharma_sales_stg.clean_sales
GROUP BY npi, drug_name, region;

-- =========================================================
-- 4️ INT Layer – Rolling Metrics
-- =========================================================

-- Rolling metrics: R3M, R6M, YTD
WITH rolling AS (
    SELECT *,
           AVG(total_sales) OVER (
               PARTITION BY drug_name, region
               ORDER BY month
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           ) AS r3m,
           AVG(total_sales) OVER (
               PARTITION BY drug_name, region
               ORDER BY month
               ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
           ) AS r6m,
           SUM(total_sales) OVER (
               PARTITION BY drug_name, region, EXTRACT(YEAR FROM month)
           ) AS ytd
    FROM pharma_sales_fact.monthly_fact
)
SELECT * INTO pharma_sales_int.rolling_metrics
FROM rolling;

-- =========================================================
-- 5️ MDM Layer – Master Data
-- =========================================================

-- Drug master with classification
CREATE TABLE pharma_sales_mdm.drug_mdm AS
SELECT DISTINCT drug_name,
       CASE WHEN drug_name IN ('KADCYLA','TRASTUZUMAB') THEN 'TARGET'
            ELSE 'COMPETITOR' END AS drug_type
FROM pharma_sales_stg.clean_sales;

-- Region & Provider master
CREATE TABLE pharma_sales_mdm.region_mdm AS
SELECT DISTINCT region FROM pharma_sales_stg.clean_sales;

CREATE TABLE pharma_sales_mdm.provider_mdm AS
SELECT DISTINCT npi FROM pharma_sales_stg.clean_sales;

-- =========================================================
-- 6️ BUS Layer – Business Rules & Joins
-- =========================================================

-- Oncology sales + competitor join
CREATE TABLE pharma_sales_bus.oncology_sales AS
SELECT r.*, d.drug_type, p.rank_trx, p.rank_sales
FROM pharma_sales_int.rolling_metrics r
LEFT JOIN pharma_sales_mdm.drug_mdm d ON r.drug_name = d.drug_name
LEFT JOIN pharma_sales_fact.provider_fact p 
       ON r.drug_name = p.drug_name AND r.region = p.region;

-- Portfolio summary
CREATE TABLE pharma_sales_bus.portfolio_summary AS
SELECT drug_name, drug_type,
       SUM(total_sales) AS total_sales,
       SUM(total_trx) AS total_trx,
       SUM(total_nrx) AS total_nrx,
       SUM(total_cbnrx) AS total_cbnrx
FROM pharma_sales_bus.oncology_sales
GROUP BY drug_name, drug_type;

-- =========================================================
-- 7️ RPT Layer – KPIs & Reporting
-- =========================================================

-- KPI Table: TRx, NRx, CbNrx, R3M, R6M, YTD, Market Share, Rank, MoM Growth
CREATE TABLE pharma_sales_rpt.kpi_monthly AS
WITH ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY region, month ORDER BY total_sales DESC) AS rank_region,
           LAG(total_sales) OVER (PARTITION BY drug_name, region ORDER BY month) AS prev_month_sales
    FROM pharma_sales_bus.oncology_sales
)
SELECT month, drug_name, region,
       SUM(total_trx) AS trx,
       SUM(total_nrx) AS nrx,
       SUM(total_cbnrx) AS cbnrx,
       SUM(total_sales) AS total_sales,
       AVG(total_sales) OVER (PARTITION BY drug_name, region ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS r3m,
       AVG(total_sales) OVER (PARTITION BY drug_name, region ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS r6m,
       SUM(total_sales) OVER (PARTITION BY drug_name, region, EXTRACT(YEAR FROM month)) AS ytd_sales,
       total_sales / SUM(total_sales) OVER (PARTITION BY region, month) AS market_share,
       rank_region AS rank,
       (total_sales - prev_month_sales)/NULLIF(prev_month_sales,0) AS mom_growth
FROM ranked
GROUP BY month, drug_name, region, total_sales, prev_month_sales, rank_region;