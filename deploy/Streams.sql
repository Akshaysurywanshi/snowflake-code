-------------
-- Streams  |
------------|

create or replace database myown_db

-- create schema for streams
create schema if not exists mystreams;

-- create schema for source tables
create schema if not exists stage_tbls;

-- create schema for target tables
create schema if not exists Intg_tbls;

-- create sample table --source table in stage schema
create or replace table stage_tbls.stg_empl
(EMPID int,
EMPNAME varchar(30),
SALARY float,
AGE int,
DEPT varchar(10),
LOCATION varchar(20)
);


-- create stream on above source table
create or replace stream mystreams.stream_empl on table stage_tbls.stg_empl;

create or replace stream mystreams.stream_empl_2 on table stage_tbls.stg_empl;

show streams in schema mystreams;

select * from mystreams.stream_empl;


-- create a target table  -- final table in integration schema
create or replace table intg_tbls.empl
(EMPID int,
EMPNAME varchar(30),
SALARY float,
AGE int,
DEPT varchar(10),
LOCATION varchar(20),
insert_dt DATE,
lst_updt_dt DATE
);


----------------|
-- ONLY INSERTS |
----------------|

-- insert some data into stage source table
insert into stage_tbls.stg_empl values
(1, 'Amar', 80000, 35, 'SALES', 'Bangalore'),
(2, 'Bharath', 45000, 26, 'SALES', 'Hyderabad'),
(3, 'Charan', 76000, 34, 'TECHNOLOGY', 'Chennai'),
(4, 'Divya', 52000, 28, 'HR', 'Hyderabad'),
(5, 'Gopal', 24500, 22, 'TECHNOLOGY', 'Bangalore'),
(6, 'Haritha', 42000, 27, 'HR', 'Chennai') ;

--check stage table data
select * from stage_tbls.stg_empl;

-- check stream object
select * from mystreams.stream_empl;

--consume stream object and load into final table
insert into intg_tbls.empl
(EMPID, EMPNAME, salary, age, dept, location, insert_dt, lst_updt_dt)
select empid,empname, salary, age, dept, location, current_date,null
from mystreams.stream_empl
where metadata$action = 'INSERT'
AND metadata$isupdate = 'FALSE';

-- view final target table data
select * from intg_tbls.empl

--observe stream object now
select * from mystreams.stream_empl;


--------------|
--only updates|
--------------|


select * from stage_tbls.stg_empl;


-- update 2 records in stage table
update stage_tbls.stg_empl set salary=49000 where empid=2;

update stage_tbls.stg_empl set location='pune' where empid=5;


--check stage table data
select * from stage_tbls.stg_empl;

--observe stream object now
select * from mystreams.stream_empl;


-- consume stream object and merge into final table
MERGE INTO INTG_TBLS.EMPL E
USING MYSTREAMS.STREAM_EMPL S
 ON E.EMPID = S.EMPID
WHEN MATCHED 
    AND S.METADATA$ACTION ='INSERT'
    AND S.METADATA$ISUPDATE ='TRUE'
THEN UPDATE 
    SET E.EMPNAME = S.EMPNAME,
  E.SALARY = S.SALARY,
  E.AGE = S.AGE,
  E.DEPT = S.DEPT,
  E.LOCATION = S.LOCATION,
  E.LST_UPDT_DT = CURRENT_DATE;

select * from intg_tbls.empl;