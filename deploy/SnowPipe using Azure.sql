// Create database and schema
CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.file_formats;
CREATE SCHEMA IF NOT EXISTS MYDB.external_stages;

-- create file format
create or replace file format MYDB.file_formats.fileformat_snowpipe
Type = CSV
field_delimiter= ','
skip_header = 1;

create or replace NOTIFICATION INTEGRATION snowpipe_event
ENABLED = true
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://covidadlsgen2sl.queue.core.windows.net/snowpipequeue'
AZURE_TENANT_ID = '5e1ec174-cb14-4662-8837-aaf640a8e388'

-- Register Integration
desc NOTIFICATION INTEGRATION snowpipe_event



-- create stage point

create or replace stage MYDB.external_stages.az_stage
URL = 'azure://covidadlsgen2sl.blob.core.windows.net/raw'
credentials = (azure_sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-05-30T14:33:52Z&st=2024-05-21T06:33:52Z&spr=https&sig=CBfeDXDWdRm6FtAzVOiUs9JdcBCca0cB9gUAK2MRO2I%3D')
    FILE_FORMAT = mydb.file_formats.fileformat_snowpipe;

-- List_files
list @MYDB.external_stages.az_stage


-- create destination table
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

select * from mydb.public.circuits

-- create pipe
create or replace pipe mydb.public.az_pipe
auto_ingest = true
integration = 'SNOWPIPE_EVENT'
as
copy into mydb.public.circuits
from @MYDB.external_stages.az_stage


-- To check the pipe status(checking the pipe status)
SELECT SYSTEM$PIPE_STATUS('MYDB.public.az_pipe');

ALTER PIPE az_pipe REFRESH;


-- view the copy history(copy history shows the history of all file loads and errors if any,
-- View the copy history by using below query.)
select * from table (information_schema.copy_history
                    (table_name => 'mydb.public.circuits', 
                    START_TIME => DATEADD(HOUR, -24 , current_timestamp())));


-- validate the data files(if the load operation encounters errors in the data files, the COPY_HISTORY table function
-- describes the first error encountered in each file,
-- To validate the data files, query the VALIDATE_PIPE_LOAD table.)
select * from table (information_schema.validate_pipe_load
                    (pipe_name => 'mydb.public.az_pipe',
                     START_TIME => DATEADD(HOUR, -1 , current_timestamp())));


-- to check single pipe
desc pipe mydb.public.az_pipe

-- just to check pipe files
show pipes
show pipes in database mydb;
