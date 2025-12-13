-- =========================================================
-- FILE: aurora_fictional_bank_access_grants.sql
-- PURPOSE:
-- Grant read-only access on all schemas and tables
-- for the Aurora Fictional Bank catalog
-- =========================================================

-- ---------------------------------------------------------
-- 1. CATALOG ACCESS
-- Allows the group to see the catalog
-- ---------------------------------------------------------
GRANT USE CATALOG ON CATALOG aurora_fictional_bank
TO `data_engineering_team`;

-- ---------------------------------------------------------
-- 2. SCHEMA ACCESS
-- Required to see schemas and objects inside them
-- ---------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA aurora_fictional_bank.master
TO `data_engineering_team`;

GRANT USE SCHEMA ON SCHEMA aurora_fictional_bank.reference
TO `data_engineering_team`;

GRANT USE SCHEMA ON SCHEMA aurora_fictional_bank.core_banking
TO `data_engineering_team`;

GRANT USE SCHEMA ON SCHEMA aurora_fictional_bank.credit
TO `data_engineering_team`;

GRANT USE SCHEMA ON SCHEMA aurora_fictional_bank.cards
TO `data_engineering_team`;

-- ---------------------------------------------------------
-- 3. TABLE READ ACCESS (ALL CURRENT TABLES)
-- Allows SELECT queries on all existing tables
-- ---------------------------------------------------------
GRANT SELECT ON TABLE bankof420.bank_master.customers
TO `data_engineering_team`;
-- Repeat the above for each table in your schemas

-- ---------------------------------------------------------
-- 4. FUTURE TABLE ACCESS (AUTO-GRANT)
-- Ensures new tables are automatically accessible
-- ---------------------------------------------------------

-- ---------------------------------------------------------
-- 5. VERIFICATION
-- ---------------------------------------------------------
SHOW GRANTS TO data_engineering_team