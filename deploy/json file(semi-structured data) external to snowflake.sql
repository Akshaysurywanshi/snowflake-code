-- Create a storage integration object
CREATE or replace STORAGE INTEGRATION azsf_jana_feb22
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '5e1ec174-cb14-4662-8837-aaf640a8e388'
  STORAGE_ALLOWED_LOCATIONS = ('azure://xml.blob.core.windows.net/data/pets_data.json');

  

--processing semi-structured data

create or replace schema mydb.extn_stages;
create or replace schema mydb.stage_tbls;
create or replace schema mydb.integration_tbls;

-- creating file format object
create or replace file format mydb.extn_stages.file_format_json
    TYPE = JSON;

-- creating stage object
create or replace stage mydb.extn_stages.stage_json
storage_integration = azsf_jana_feb22
url= 'azure://xml.blob.core.windows.net/data/pets_data.json'


-- Listing files in th stage

list @mydb.extn_stages.stage_json



-- Creating stage table to store Raw data

create or replace table mydb.stage_tbls.pets_data_json_raw
(raw_file variant)



--Copy the RAW data into a Stage Table
COPY INTO mydb.stage_tbls.PETS_DATA_JSON_RAW 
    FROM @mydb.extn_stages.stage_json
    file_format= mydb.extn_stages.FILE_FORMAT_JSON

-- view raw table data
select * from mydb.stage_tbls.PETS_DATA_JSON_RAW


-- extracting single column
select RAW_FILE:Name :: string as Name from mydb.stage_tbls.PETS_DATA_JSON_RAW



SELECT 
    value:Name::STRING AS Name
FROM 
    mydb.stage_tbls.PETS_DATA_JSON_RAW,
    LATERAL FLATTEN(input => RAW_FILE)
;