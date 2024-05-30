-- Create a storage integration object
CREATE or replace STORAGE INTEGRATION snow_azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '5e1ec174-cb14-4662-8837-aaf640a8e388'
  STORAGE_ALLOWED_LOCATIONS = ('azure://covidadlsgen2sl.blob.core.windows.net/raw/circuits.csv');




  -- Describe integration object
DESC STORAGE INTEGRATION snow_azure_int;



// Create database and schema
CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.file_formats;
CREATE SCHEMA IF NOT EXISTS MYDB.external_stages;

// Create file format object
CREATE OR REPLACE file format mydb.file_formats.csv_fileformat
    type = csv
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE;    
    
// Create stage object with integration object & file format object
CREATE OR REPLACE STAGE mydb.external_stages.stg_azure_cont
    URL = 'azure://covidadlsgen2sl.blob.core.windows.net/raw/circuits.csv'
    STORAGE_INTEGRATION = snow_azure_int
    FILE_FORMAT = mydb.file_formats.csv_fileformat ;


//Listing files under your azure containers
list @mydb.external_stages.stg_azure_cont;


// Create a table first

CREATE or replace TABLE mydb.public.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
);


MYDB.EXTERNAL_STAGES.STG_AZURE_CONT

// Use Copy command to load the files
COPY INTO mydb.public.circuits
    FROM @mydb.external_stages.stg_azure_cont
    file_format = mydb.file_formats.csv_fileformat
 
//Validate the data
SELECT * FROM mydb.public.customer_data;