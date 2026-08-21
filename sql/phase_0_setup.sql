-- =============================================================================
-- Phase 0 Setup SQL
-- Run this entire file in the Databricks SQL editor as a one-time setup.
-- Requires: CATALOG CREATE privilege on the metastore.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1: Create catalogs
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS dwh_udev;
CREATE CATALOG IF NOT EXISTS dwh_upro;


-- -----------------------------------------------------------------------------
-- STEP 2: Create schemas in dev catalog
-- -----------------------------------------------------------------------------
USE CATALOG dwh_udev;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS marketplace;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS dwh_internal;
CREATE SCHEMA IF NOT EXISTS seeds;


-- -----------------------------------------------------------------------------
-- STEP 3: Create schemas in prod catalog
-- -----------------------------------------------------------------------------
USE CATALOG dwh_upro;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS marketplace;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS dwh_internal;
CREATE SCHEMA IF NOT EXISTS seeds;


-- -----------------------------------------------------------------------------
-- STEP 4: Create raw source tables in dev catalog
-- These simulate the raw landing zone data from upstream systems.
-- In a real project these would be populated by ingestion pipelines.
-- -----------------------------------------------------------------------------
USE CATALOG dwh_udev;


-- HR: raw employee records (one row per employee per day = SCD snapshot pattern)
CREATE TABLE IF NOT EXISTS staging.raw_hr__employees (
    employee_id   BIGINT,
    full_name     STRING,
    department_id INT,
    salary        DECIMAL(10,2),
    hire_date     DATE,
    status        STRING,     -- 'active' | 'terminated'
    site_id       INT,        -- which marketplace site this employee belongs to
    event_date    DATE        -- the partition column: date of the snapshot
) USING DELTA PARTITIONED BY (event_date);


-- Marketplace: raw listing records
CREATE TABLE IF NOT EXISTS staging.raw_mkt__listings (
    listing_id  BIGINT,
    seller_id   BIGINT,
    category_id INT,
    price       DECIMAL(10,2),
    status      STRING,       -- 'active' | 'sold' | 'expired'
    site_id     INT,
    listed_date DATE,
    event_date  DATE
) USING DELTA PARTITIONED BY (event_date);


-- Payments: raw transaction records
CREATE TABLE IF NOT EXISTS staging.raw_pay__transactions (
    transaction_id BIGINT,
    buyer_id       BIGINT,
    seller_id      BIGINT,
    listing_id     BIGINT,
    amount         DECIMAL(10,2),
    currency_code  STRING,     -- e.g. 'EUR'
    status         STRING,     -- 'completed' | 'failed' | 'refunded' | 'pending'
    site_id        INT,
    event_date     DATE
) USING DELTA PARTITIONED BY (event_date);


-- -----------------------------------------------------------------------------
-- STEP 5: Insert sample data
-- Covers 3 event_dates: 2026-08-18, 2026-08-19, 2026-08-20
-- This gives us enough partitions to test the incremental lookback window logic.
-- -----------------------------------------------------------------------------

INSERT INTO staging.raw_hr__employees VALUES
    (1, 'Alice Smith',  10, 75000, '2020-01-15', 'active',     1, '2026-08-18'),
    (2, 'Bob Jones',    20, 65000, '2019-06-01', 'active',     1, '2026-08-18'),
    (3, 'Carol White',  10, 80000, '2021-03-10', 'terminated', 2, '2026-08-18'),
    (4, 'Dave Brown',   30, 55000, '2022-11-20', 'active',     2, '2026-08-19'),
    (5, 'Eve Davis',    20, 90000, '2018-07-04', 'active',     1, '2026-08-19'),
    (6, 'Frank Miller', 10, 72000, '2023-02-14', 'active',     3, '2026-08-20');

INSERT INTO staging.raw_mkt__listings VALUES
    (101, 1, 10, 150.00, 'active',  1, '2026-08-15', '2026-08-18'),
    (102, 2, 20, 299.99, 'sold',    1, '2026-08-10', '2026-08-18'),
    (103, 4, 10,  75.50, 'active',  2, '2026-08-17', '2026-08-19'),
    (104, 5, 30, 499.00, 'active',  1, '2026-08-18', '2026-08-19'),
    (105, 1, 20, 199.00, 'expired', 3, '2026-08-01', '2026-08-20');

INSERT INTO staging.raw_pay__transactions VALUES
    (1001, 3, 1, 102, 299.99, 'EUR', 'completed', 1, '2026-08-18'),
    (1002, 2, 4, 103,  75.50, 'EUR', 'completed', 2, '2026-08-19'),
    (1003, 1, 5, 104, 499.00, 'EUR', 'failed',    1, '2026-08-19'),
    (1004, 5, 1, 101, 150.00, 'EUR', 'completed', 1, '2026-08-20');


-- -----------------------------------------------------------------------------
-- STEP 6: Verify row counts
-- Expected: employees=6, listings=5, transactions=4
-- -----------------------------------------------------------------------------
SELECT 'raw_hr__employees'     AS table_name, COUNT(*) AS row_count FROM dwh_udev.staging.raw_hr__employees
UNION ALL
SELECT 'raw_mkt__listings'     AS table_name, COUNT(*) AS row_count FROM dwh_udev.staging.raw_mkt__listings
UNION ALL
SELECT 'raw_pay__transactions' AS table_name, COUNT(*) AS row_count FROM dwh_udev.staging.raw_pay__transactions;
