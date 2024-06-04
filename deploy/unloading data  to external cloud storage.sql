==================================================
Unloading data to external cloud storage locations
===================================================
// Create required Database and Schemas
CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.EXT_STAGES;
CREATE SCHEMA IF NOT EXISTS MYDB.FILE_FORMATS;


CREATE or replace STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '5e1ec174-cb14-4662-8837-aaf640a8e388'
  STORAGE_ALLOWED_LOCATIONS = ('azure://covidadlsgen2sl.blob.core.windows.net/raw/output/');


// Create file format object
CREATE OR REPLACE FILE FORMAT MYDB.FILE_FORMATS.CSV_FILEFORMAT
    type = csv
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE; 


CREATE OR REPLACE STAGE MYDB.EXT_STAGES.MYS3_OUTPUT
    URL = 'azure://covidadlsgen2sl.blob.core.windows.net/raw/output/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MYDB.FILE_FORMATS.CSV_FILEFORMAT ;


// Generate files and store them in the stage location
COPY INTO @MYDB.EXT_STAGES.MYS3_OUTPUT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;


list @MYDB.EXT_STAGES.MYS3_OUTPUT

--------------------------------------------------------OR----------------------------------------------------------




-- Unloading the data in external storage
-- specify the filename in the copy command
COPY INTO @MYDB.EXT_STAGES.MYS3_OUTPUT/customer
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;


-- If we want to overwrite existing file we can set that to TRUE
COPY INTO @MYDB.EXT_STAGES.MYS3_OUTPUT/customer
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
MAX_FILE_SIZE=20000000 --(2mb)
overwrite = True;


--if we want to generate single file then
COPY INTO @MYDB.EXT_STAGES.MYS3_OUTPUT/customer
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
SINGLE = TRUE;

--if we want detailed output
COPY INTO @MYDB.EXT_STAGES.MYS3_OUTPUT/cust_data
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
Detailed_output = TRUE;

-- detailed o/p:
FILE_NAME	              FILE_SIZE	      ROW_COUNT
cust_data_0_1_0.csv.gz	    2710445	         45000
cust_data_0_0_0.csv.gz	    1805774	         30000
cust_data_0_3_0.csv.gz	4519924	75000
