-- user stage(@~stage)------------------------------------------------------------------------------

create schema if not exists mydb.internal_stage

-- go to CMD and run these commands
-- 1.connect through snowsql
-- 2.use database or schema then run PUT command while running put command provide data path from your local data

CREATE or replace TABLE mydb.internal_stage.circuit3 (
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

put file://C:\Users\Lenovo\Downloads\circuit3.csv @~/staged;


--Table Stage (@%)------------------------------------------------------------------------------------
-- here whatever is table name same name is stage name
-- we have to create table first through cmd then run tis put command

//Create customer_data_table to load files from internal stages
CREATE or replace TABLE mydb.internal_stage.customer_data_table (
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


//Put your files into table internal stage
-- before  run this command on cmd if this schema or table is not available then we get error
put file://C:\Users\janar\OneDrive\Documents\Files\customer_data_table.csv @%customer_data_table;



-- Named internal stage------------------------------------------------------------------------------------------
//create schema for internal stages
create schema if not exists mydb.inernal_stage

create or replace stage mydb.internal_stages.named_customer_stage;
drop table mydb.internal_stage.circuit;

CREATE or replace TABLE mydb.internal_stage.named_customer_stage (
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


-- //load all files data to the table--------------------------------------------------------------------

copy into mydb.internal_stage.circuit3
from @~/staged/circuit3.csv
file_format = (type =csv field_delimiter = ',' skip_header = 1);


list @~/stage
select * from circuit3




copy into mydb.internal_stage.customer_data_table
from @%customer_data_table/circuit2.csv
file_format = (type =csv field_delimiter = ',' skip_header = 1);

list @%customer_data_table
select * from customer_data_table





copy into mydb.internal_stage.named_customer_stage
from @mydb.internal_stage.named_customer_stage/circuits.csv
file_format = (type =csv field_delimiter = ',' skip_header = 1);

list @mydb.internal_stage.named_customer_stage
select * from mydb.internal_stage.named_customer_stage