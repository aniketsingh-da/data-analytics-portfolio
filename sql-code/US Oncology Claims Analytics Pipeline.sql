/***************************************************************************************************
 * FILE NAME       : US Oncology Claims Analytics Pipeline.sql
 * DESCRIPTION     : Production-grade SQL pipeline for FRUZAQLA (fruquintinib) US oncology claims data.
 *                   Implements a full layered architecture: LDG → STG → FACT → INT → MDM → BUS → RPT.
 *                   Calculates weekly and monthly KPIs including adherence, persistence, switch rate,
 *                   provider adoption, payer mix, rolling R3M/R6M trends, YoY patient growth, state-wise utilization, and more.
 * DATA SOURCE     : Azure Blob Storage (raw pharmacy and medical claims)
 * PLATFORM        : Azure SQL Database
 * ORCHESTRATION   : Azure Data Factory (ADF) with Logic Apps for notifications
 * LAST UPDATED    : 2026-03-17
 * AUTHOR          : Aniket Singh
 ***************************************************************************************************/


-- ===========================================
-- LDG Layer: Raw Claims Ingestion
-- Contains raw pharmacy & medical claims directly from Blob storage
-- ===========================================
CREATE TABLE ldg.raw_claims_rx (
    claim_id BIGINT PRIMARY KEY,            -- Unique claim identifier
    patient_id BIGINT,                       -- De-identified patient ID
    provider_id BIGINT,                      -- NPI of provider
    payer_id BIGINT,                         -- Payer identifier
    drug_ndc VARCHAR(20),                    -- NDC code of drug
    fill_date DATE,                          -- Fill / service date
    days_supply INT,                          -- Days supplied
    quantity INT,                             -- Units supplied
    cost DECIMAL(18,2),                       -- Cost of claim
    service_code VARCHAR(20),                 -- J-code or procedure code
    created_at DATETIME DEFAULT GETDATE()     -- Load timestamp
);

-- QC log for raw file ingestion
CREATE TABLE ldg.blob_qc_log (
    load_id BIGINT PRIMARY KEY IDENTITY,
    file_name VARCHAR(255),
    load_date DATETIME DEFAULT GETDATE(),
    row_count INT,
    status VARCHAR(20)
);

-- ===========================================
-- STG Layer: Clean & QC Claims
-- Remove invalid, null, negative claims and map NDC codes to drugs
-- ===========================================
CREATE TABLE stg.clean_claims AS
SELECT *
FROM ldg.raw_claims_rx
WHERE patient_id IS NOT NULL
  AND provider_id IS NOT NULL
  AND drug_ndc IS NOT NULL
  AND quantity > 0;

-- Identify duplicate claims
CREATE TABLE stg.qc_duplicate_claims AS
SELECT claim_id, COUNT(*) AS duplicate_count
FROM stg.clean_claims
GROUP BY claim_id
HAVING COUNT(*) > 1;

-- Map NDC codes to drugs using MDM
CREATE TABLE stg.drug_mapping AS
SELECT c.*, d.drug_name, d.brand_name
FROM stg.clean_claims c
LEFT JOIN mdm.drug_mdm d
    ON c.drug_ndc = d.ndc_code;
	
-- ===========================================
-- FACT Layer: Atomic Claims Facts
-- Includes patient, provider, drug, payer, and claim type
-- ===========================================
CREATE TABLE fact.claims_fact AS
SELECT
    claim_id,
    patient_id,
    provider_id,
    payer_id,
    drug_name,
    brand_name,
    fill_date,
    quantity,
    days_supply,
    cost,
    -- Classify claim as New vs Refill
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY patient_id, drug_name ORDER BY fill_date) = 1 THEN 'New'
        ELSE 'Refill'
    END AS claim_type
FROM stg.drug_mapping;

-- Patient-level aggregation
CREATE TABLE fact.patient_fact AS
SELECT 
    patient_id,
    MIN(fill_date) AS first_claim_date,
    MAX(fill_date) AS last_claim_date,
    COUNT(claim_id) AS total_claims,
    SUM(quantity) AS total_quantity
FROM fact.claims_fact
GROUP BY patient_id;

-- ===========================================
-- INT Layer: Rolling Metrics, Adherence, Persistence
-- ===========================================

-- Rolling 3-month claims
CREATE TABLE int.rolling_r3m AS
SELECT 
    drug_name,
    DATEADD(MONTH, DATEDIFF(MONTH, 0, fill_date), 0) AS month_start,
    COUNT(claim_id) AS claims_r3m,
    AVG(COUNT(claim_id)) OVER (PARTITION BY drug_name 
                               ORDER BY DATEADD(MONTH, DATEDIFF(MONTH, 0, fill_date), 0) 
                               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_r3m_avg
FROM fact.claims_fact
GROUP BY drug_name, DATEADD(MONTH, DATEDIFF(MONTH, 0, fill_date), 0);

-- Rolling 6-month claims
CREATE TABLE int.rolling_r6m AS
SELECT 
    drug_name,
    DATEADD(MONTH, DATEDIFF(MONTH, 0, fill_date), 0) AS month_start,
    COUNT(claim_id) AS claims_r6m,
    AVG(COUNT(claim_id)) OVER (PARTITION BY drug_name 
                               ORDER BY DATEADD(MONTH, DATEDIFF(MONTH, 0, fill_date), 0) 
                               ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_r6m_avg
FROM fact.claims_fact
GROUP BY drug_name, DATEADD(MONTH, DATEDIFF(MONTH, 0, fill_date), 0);

-- Adherence (PDC) calculation
CREATE TABLE int.adherence_metrics AS
SELECT 
    patient_id,
    drug_name,
    SUM(days_supply) * 1.0 / DATEDIFF(DAY, MIN(fill_date), MAX(fill_date)) AS pdc
FROM fact.claims_fact
GROUP BY patient_id, drug_name;

-- Persistence Flag (>=90 days of therapy)
CREATE TABLE int.persistence_flags AS
SELECT 
    patient_id,
    drug_name,
    CASE WHEN DATEDIFF(DAY, MIN(fill_date), MAX(fill_date)) >= 90 THEN 1 ELSE 0 END AS persistent_flag
FROM fact.claims_fact
GROUP BY patient_id, drug_name;


-- ===========================================
-- MDM Layer: Reference tables for mapping & validation
-- ===========================================

-- Drug Master
CREATE TABLE mdm.drug_mdm (
    drug_id INT PRIMARY KEY,
    drug_name VARCHAR(100),
    brand_name VARCHAR(100),
    ndc_code VARCHAR(20)
);

-- Provider Master
CREATE TABLE mdm.provider_mdm (
    provider_id BIGINT PRIMARY KEY,
    provider_name VARCHAR(255),
    specialty VARCHAR(50),
    state VARCHAR(20)
);

-- Payer Master
CREATE TABLE mdm.payer_mdm (
    payer_id INT PRIMARY KEY,
    payer_name VARCHAR(100),
    payer_type VARCHAR(50)  -- Commercial, Medicare, Medicaid
);

-- Geography Master
CREATE TABLE mdm.geography_mdm (
    provider_id BIGINT PRIMARY KEY,
    state VARCHAR(50),
    region VARCHAR(50)
);

-- ===========================================
-- BUS Layer: Business Rules
-- - New vs Refill
-- - Line of Therapy
-- - Switch between drugs
-- ===========================================

-- New vs Refill already computed in FACT

-- Switch Flag: patient moving from competitor to FRUZAQLA
CREATE TABLE bus.switch_flag AS
SELECT
    patient_id,
    drug_name,
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM fact.claims_fact c2
            WHERE c2.patient_id = c1.patient_id 
              AND c2.drug_name IN ('Competitor1', 'Competitor2') 
              AND c2.fill_date < c1.fill_date
        ) THEN 1 ELSE 0
    END AS switched
FROM fact.claims_fact c1
WHERE drug_name='FRUZAQLA';


-- ===========================================
-- RPT Layer: Weekly & Monthly KPIs
-- All KPIs calculated in single reporting layer
-- ===========================================

-- Monthly KPI Table
CREATE TABLE rpt.kpi_claims_monthly AS
SELECT 
    DATEPART(YEAR, fill_date) AS year,
    DATEPART(MONTH, fill_date) AS month,
    drug_name,
    
    -- Total claims
    COUNT(claim_id) AS total_claims,
    
    -- Unique patients
    COUNT(DISTINCT patient_id) AS unique_patients,
    
    -- New Patient Starts
    SUM(CASE WHEN claim_type='New' THEN 1 ELSE 0 END) AS new_patient_starts,
    
    -- Refill Rate
    SUM(CASE WHEN claim_type='Refill' THEN 1 ELSE 0 END) * 1.0 / COUNT(claim_id) AS refill_rate,
    
    -- Adherence (PDC)
    AVG(a.pdc) AS adherence_pdc,
    
    -- Persistence Rate
    SUM(persistent_flag) * 1.0 / COUNT(DISTINCT patient_id) AS persistence_rate,
    
    -- Discontinuation Rate
    1.0 - (SUM(persistent_flag) * 1.0 / COUNT(DISTINCT patient_id)) AS discontinuation_rate,
    
    -- Avg Claims per Patient
    COUNT(claim_id) * 1.0 / COUNT(DISTINCT patient_id) AS avg_claims_per_patient,
    
    -- Payer Mix %
    SUM(CASE WHEN mdm.payer_type='Commercial' THEN 1 ELSE 0 END) * 100.0 / COUNT(claim_id) AS payer_commercial_pct,
    SUM(CASE WHEN mdm.payer_type='Medicare' THEN 1 ELSE 0 END) * 100.0 / COUNT(claim_id) AS payer_medicare_pct,
    SUM(CASE WHEN mdm.payer_type='Medicaid' THEN 1 ELSE 0 END) * 100.0 / COUNT(claim_id) AS payer_medicaid_pct,
    
    -- State-wise utilization
    geo.state,
    COUNT(claim_id) AS state_total_claims,
    
    -- R3M & R6M Claims Trend
    AVG(rr3.claims_r3m) AS r3m_claims_avg,
    AVG(rr6.claims_r6m) AS r6m_claims_avg,
    
    -- YoY Patient Growth
    (COUNT(DISTINCT patient_id) - LAG(COUNT(DISTINCT patient_id)) OVER (PARTITION BY drug_name ORDER BY DATEPART(YEAR, fill_date), DATEPART(MONTH, fill_date))) 
    * 1.0 / NULLIF(LAG(COUNT(DISTINCT patient_id)) OVER (PARTITION BY drug_name ORDER BY DATEPART(YEAR, fill_date), DATEPART(MONTH, fill_date)),0) AS yoy_patient_growth,
    
    -- Provider Adoption Rate
    COUNT(DISTINCT provider_id) * 1.0 / (SELECT COUNT(*) FROM mdm.provider_mdm) AS provider_adoption_rate,
    
    -- Switch Rate
    SUM(s.switched) * 1.0 / COUNT(DISTINCT patient_id) AS switch_rate

FROM fact.claims_fact f
LEFT JOIN int.adherence_metrics a ON f.patient_id = a.patient_id AND f.drug_name = a.drug_name
LEFT JOIN int.persistence_flags p ON f.patient_id = p.patient_id AND f.drug_name = p.drug_name
LEFT JOIN int.rolling_r3m rr3 ON f.drug_name = rr3.drug_name
LEFT JOIN int.rolling_r6m rr6 ON f.drug_name = rr6.drug_name
LEFT JOIN mdm.payer_mdm mdm ON f.payer_id = mdm.payer_id
LEFT JOIN mdm.geography_mdm geo ON f.provider_id = geo.provider_id
LEFT JOIN bus.switch_flag s ON f.patient_id = s.patient_id AND f.drug_name = s.drug_name

GROUP BY DATEPART(YEAR, fill_date), DATEPART(MONTH, fill_date), drug_name, geo.state;

----------------------------------------
-- Weekly KPI Table (similar to monthly)
----------------------------------------

CREATE TABLE rpt.kpi_claims_weekly AS
SELECT 
    DATEPART(YEAR, fill_date) AS year,
    DATEPART(WEEK, fill_date) AS week,
    drug_name,
    COUNT(claim_id) AS total_claims,
    COUNT(DISTINCT patient_id) AS unique_patients,
    SUM(CASE WHEN claim_type='New' THEN 1 ELSE 0 END) AS new_patient_starts,
    -- Refill rate, adherence, persistence, switch rate etc.
    SUM(CASE WHEN claim_type='Refill' THEN 1 ELSE 0 END) * 1.0 / COUNT(claim_id) AS refill_rate,
    AVG(a.pdc) AS adherence_pdc,
    SUM(persistent_flag) * 1.0 / COUNT(DISTINCT patient_id) AS persistence_rate,
    SUM(s.switched) * 1.0 / COUNT(DISTINCT patient_id) AS switch_rate
FROM fact.claims_fact f
LEFT JOIN int.adherence_metrics a ON f.patient_id = a.patient_id AND f.drug_name = a.drug_name
LEFT JOIN int.persistence_flags p ON f.patient_id = p.patient_id AND f.drug_name = p.drug_name
LEFT JOIN bus.switch_flag s ON f.patient_id = s.patient_id AND f.drug_name = s.drug_name
GROUP BY DATEPART(YEAR, fill_date), DATEPART(WEEK, fill_date), drug_name;