-- =============================================================================
-- Ad-hoc Cleanup SQL
-- Run this BEFORE re-running phase_0_setup.sql if you executed an older
-- version of the setup script that placed raw tables in dwh_udev.staging.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Remove incorrectly placed raw tables from dwh_udev.staging
-- These were created by the old phase_0_setup.sql before the catalog
-- restructure that moved raw tables into dwh_raw.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dwh_udev.staging.raw_hr__employees;
DROP TABLE IF EXISTS dwh_udev.staging.raw_mkt__listings;
DROP TABLE IF EXISTS dwh_udev.staging.raw_pay__transactions;


-- -----------------------------------------------------------------------------
-- Verify they are gone
-- -----------------------------------------------------------------------------
SHOW TABLES IN dwh_udev.staging;
-- Expected: empty (no tables yet — dbt will create views here in Phase 1)
