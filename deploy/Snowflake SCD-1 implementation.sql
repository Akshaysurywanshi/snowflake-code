create database if not exists emp;
create schema if not exists staging;
create schema if not exists target;

-- //create a stage table
create OR replace table staging.stg_empl
(
eid integer not null,
ename varchar(30),
dob Date,
mail varchar(50),
phone varchar(20),
salary integer,
dept varchar(30),
loc varchar(20),
PRIMARY KEY(EID)
);


//CREATE THE TARGET TABLE
create or replace table target.empl_scd1
(
emp_id integer not null,
emp_name varchar(30),
date_of_birth Date,
email_id varchar(50),
phone_number varchar(20),
salary integer,
department varchar(30),
work_location varchar(20),
insert_ts TIMESTAMP,
last_update_ts TIMESTAMP,
PRIMARY KEY(emp_id)
);


-- create stream on stage table
create stream staging.stream_stg_empl on table staging.stg_empl;

-- //merge the changes to target table
merge into target.empl_scd1 TGT
using staging.stream_stg_empl STR
on tgt.emp_id = str.eid

when matched
and str.metadata$action = 'insert'
and str.metadata$isupdate = 'TRUE'
and (tgt.email_id <> str.mail or
     tgt.phone_number <> str.phone or
     tgt.salary <> str.salary or
     tgt.department <> str.dept or
     tgt.work_location <> str.loc )
    
Then update set
    tgt.email_id = str.mail,
    tgt.phone_number = str.phone,
    tgt.salary = str.salary,
    tgt.department = str.dept,
    tgt.work_location = str.loc,
    tgt.last_update_ts = current_timestamp

When Not Matched Then
insert (emp_id, emp_name, date_of_birth, email_id, phone_number, salary, department, work_location, insert_ts, last_update_ts)
values (str.eid, str.ename, str.dob, str.mail, str.phone, str.salary, str.dept , str.loc, current_timestamp, current_timestamp);



-- schedule this merger query using Task
create or replace task target.task_empl_data_load
    schedule = '2 MINUTES'
    WHEN SYSTEM$STREAM_HAS_DATA ('staging.stream_stg_empl')
AS
merge into target.empl_scd1 TGT
using staging.stream_stg_empl STR
on tgt.emp_id = str.eid

when matched
and str.metadata$action = 'insert'
and str.metadata$isupdate = 'TRUE'
and (tgt.email_id <> str.mail or
     tgt.phone_number <> str.phone or
     tgt.salary <> str.salary or
     tgt.department <> str.dept or
     tgt.work_location <> str.loc )
    
Then update set
    tgt.email_id = str.mail,
    tgt.phone_number = str.phone,
    tgt.salary = str.salary,
    tgt.department = str.dept,
    tgt.work_location = str.loc,
    tgt.last_update_ts = current_timestamp

When Not Matched Then
insert (emp_id, emp_name, date_of_birth, email_id, phone_number, salary, department, work_location, insert_ts, last_update_ts)
values (str.eid, str.ename, str.dob, str.mail, str.phone, str.salary, str.dept , str.loc, current_timestamp, current_timestamp);


-- //start the task
Alter task target.task_empl_data_load Resume


-- data load into stage table
insert into staging.stg_empl values
(1,'Rahul sharma', '1986-04-15', 'rahul_sharma@gmail.com', '9988776655', 92000, 'Administration', 'Bangalore'),
(2,'Renuka devi', '1993-10-19', 'renuka1993@yahoo.com', '+91 9911882255', 61000, 'sales', 'Hyderabad' ),
(3, 'Kamalesh', '1991-02-08', 'kamal91@yahoo.com', '9182736450', 59000, 'Sales', 'Chennai' ),
(4,'Arun Kumar', '1989-05-20', 'arun_kumar@gmail.com', '901-287-3465', 74500, 'IT', 'Bangalore')


select * from staging.stg_empl;


-- Observe streams now with change capture
select * from staging.stream_stg_empl;

-- After first run
-- verify the data in target table
select * from target.empl_scd1;

-- Observe the stream now after consuming the changes
select * from staging.stream_stg_empl;


----------------------------------------------------------------------------------------
--make changes to the stage table (assume it is truncate and load with new and updated records)
insert into staging.stg_empl values
(5, 'Deepika Kaur', '1995-09-03', 'deepikakaur@gmail.com', '9871236054', 58000, 'IT','Pune');

update staging.stg_empl set phone = '9911882255' where eid =2;

-- Observe streams now with change capture
select * from staging.stream_stg_empl;

-- After first run
-- verify the data in target table
select * from target.empl_scd1;

-- Observe the stream now after consuming the changes
select * from staging.stream_stg_empl;




-- Stop or Drop the task, otherwise all your tree credits will be consumed
Alter Task target.task_empl_data_load Suspend;

