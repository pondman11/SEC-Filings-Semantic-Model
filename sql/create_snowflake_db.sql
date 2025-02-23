CREATE DATABASE IF NOT EXISTS SEC_FILINGS_DB;
CREATE SCHEMA IF NOT EXISTS SEC_FILINGS_DB.RAW_DATA;
CREATE SCHEMA IF NOT EXISTS SEC_FILINGS_DB.PROCESSED_DATA;
CREATE SCHEMA IF NOT EXISTS SEC_FILINGS_DB.SEC_SEMANTIC_MODEL;

CREATE OR REPLACE STAGE SEC_FILINGS_DB.RAW_DATA."10_K_FILINGS" ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = TRUE );
CREATE OR STAGE IF NOT EXISTS STAGE SEC_FILINGS_DB.RAW_DATA.UDFS; 
CREATE STAGE IF NOT EXISTS SEC_FILINGS_DB.RAW_DATA."10_K_METRICS";


