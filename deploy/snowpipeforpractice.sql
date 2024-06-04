use mydb;

create or replace storage integration snowpipe_az_integration
Type = External_stage
storage_provider = AZURE
Enabled = TRUE
Azure_Tenant_id = '5e1ec174-cb14-4662-8837-aaf640a8e388'
Storage_Allowed_Locations = ('azure://ordercsv.blob.core.windows.net/csv/');


desc storage integration snowpipe_az_integration

create or replace file format MYDB.file_formats.fileformat_snowpipe
Type = CSV
field_delimiter= ','
skip_header = 1;

create or replace stage MYDB.external_stages.az_stagess
Storage_Integration = snowpipe_az_integration
url = 'azure://ordercsv.blob.core.windows.net/csv/'
File_Format = MYDB.file_formats.fileformat_snowpipe;

list @MYDB.external_stages.az_stagess


create or replace NOTIFICATION INTEGRATION snowpipe_events
Enabled = true
Type = QUEUE
Notification_Provider = Azure_Storage_Queue
Azure_Storage_Queue_Primary_Uri = 'https://ordercsv.queue.core.windows.net/snowpipequeue'
Azure_Tenant_Id = '5e1ec174-cb14-4662-8837-aaf640a8e388'

desc NOTIFICATION INTEGRATION snowpipe_events

select $1,$2,$3,$4,$5,$6 from @MYDB.external_stages.az_stagess

CREATE or replace TABLE mydb.public.circuitssss (
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

select * from mydb.public.circuitssss

create or replace pipe az_pipes
auto_ingest = true
integration = snowpipe_events
as
copy into mydb.public.circuitssss
from @MYDB.external_stages.az_stagess


SELECT SYSTEM$PIPE_STATUS('MYDB.public.az_pipes');

ALTER PIPE az_pipes REFRESH;

