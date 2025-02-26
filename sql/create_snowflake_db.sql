CREATE DATABASE IF NOT EXISTS SEC_FILINGS_DB;
CREATE SCHEMA IF NOT EXISTS SEC_FILINGS_DB.RAW_DATA;
CREATE SCHEMA IF NOT EXISTS SEC_FILINGS_DB.PROCESSED_DATA;
CREATE SCHEMA IF NOT EXISTS SEC_FILINGS_DB.SEC_SEMANTIC_MODEL;

CREATE OR REPLACE STAGE SEC_FILINGS_DB.RAW_DATA."10_K_FILINGS" ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = TRUE );
CREATE STAGE IF NOT EXISTS SEC_FILINGS_DB.RAW_DATA.UDFS; 
CREATE STAGE IF NOT EXISTS SEC_FILINGS_DB.RAW_DATA."10_K_METRICS";

CREATE OR REPLACE TABLE SEC_FILINGS_DB.RAW_DATA."10-K-CHUNKS"(
    ID VARCHAR(16777216),
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the HTML file
    SIZE NUMBER(38,0), -- Size of the HTML
    FILE_URL VARCHAR(16777216), -- URL for the HTML
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CATEGORIES VARCHAR(16777216), -- Will hold the document category to enable filtering
    TICKER VARCHAR(16777216),
    "DATE" VARCHAR(16777216),
);

