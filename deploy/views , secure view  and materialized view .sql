--create database
create or replace database public_db;

use database public_db;

--create schema for view
create schema myviews;

--Requirement: Need contact details for Brazil customers.

--create customer view
create or replace view myviews.vw_customer
as
select cst.c_custkey, cst.c_name, cst.c_address, cst.c_phone from
snowflake_sample_data.tpch_sf100.customer CST
inner join snowflake_sample_data.tpch_sf100.nation ntn
on cst.c_nationkey = ntn.n_nationkey
where ntn.n_name = 'BRAZIL';


// Query the view and see query profile how it is fetching data from underlying tables
SELECT * FROM MYVIEWS.VW_CUSTOMER;

// Turno off cached results and suspend warehouse
ALTER SESSION SET USE_CACHED_RESULT=FALSE;
--(if run this query which 'cache = false' above and suspend your warehouse then that result view second time fetch result from  underlined tables not from result chache or not from local disk cache)


--Now how to share this view we created this view but we have to share ths view with that customer whoever requesting the brazil data,
//grant access to role PUBLIC
GRANT USAGE ON DATABASE public_db TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA PUBILC_DB.MYVIEWS.VW_CUSTOMER TO ROLE PUBLIC;  --role or user who needs brazil customers data
                                                            OR
                                                    USER USER_NAME


---------------|
--SECURE VIEW  |
---------------|

//requirement : Need all details of america customers

//create secure view
create secure view myviews.sec_vw_customer
as 
select cst.* from snowflake_sample_data.tpch_sf100.customer cst
inner join snowflake_sample_data.tpch_sf100. Nation ntn
on cst.c_nationkey = ntn.n_nationkey
inner join snowflake_sample_data.tpch_sf100.region rgn
on ntn.n_regionkey = rgn.r_regionkey
where rgn.r_name = 'AMERICA';

//query secure view
select * from myviews.sec_vw_customer

// Grant access to role PUBLIC
GRANT USAGE ON DATABASE PUBLIC_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA PUBLIC_DB.MYVIEWS TO ROLE PUBLIC;
GRANT SELECT ON VIEW PUBLIC_DB.MYVIEWS.SEC_VW_CUSTOMER TO ROLE PUBLIC; -- role or user who needs AMERICA customers data


// How to Identify a View is secure?
SELECT  table_catalog, table_schema, table_name, is_secure
    FROM  public_db.information_schema.views;
    
SHOW VIEWS;


// Switch to PUBLIC ROLE from ACCOUNTADMIN role and check
SHOW VIEWS; -- only owner can see the definition of view



=====================
-- MATERIALIZED VIEWS-------------------------------------------------------------------------------------------------------
=====================

//Try creating a materialize view with multiple table - it won't work because joins not allowed in materialized view
-- CREATE MATERIALIZED VIEW MYVIEWS.MAT_VW_CUSTOME
-- AS
-- SELECT CST.* FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER CST
-- INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.NATION NTN
-- ON CST.C_NATIONKEY = NTN.N_NATIONKEY
-- INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.REGION RGN
-- ON NTN.N_REGIONKEY = RGN.R_REGIONKEY
-- WHERE RGN.R_NAME='AMERICA';

// Requirement: I want to check frequently the High priority order details.

// Create materialized view
CREATE MATERIALIZED VIEW MYVIEWS.MAT_VW_ORDERS
AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS
WHERE SUBSTRING(O_ORDERPRIORITY,1,1)='2';
-- AND YEAR(O_ORDERDATE)=2022 AND MONTH(O_ORDERDATE)=7

// Query mat view and see query profile, run after some time
SELECT * FROM MYVIEWS.MAT_VW_ORDERS;

// Grant access to PUBLIC role
GRANT USAGE ON DATABASE PUBLIC_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA PUBLIC_DB.MYVIEWS TO ROLE PUBLIC;
GRANT SELECT ON VIEW PUBLIC_DB.MYVIEWS.MAT_VW_ORDERS TO ROLE PUBLIC;

// How to see the mat views?
SHOW MATERIALIZED VIEWS;

// How to check the refresh history?(when we do changes in our underline table then ithin few minutes changes happen in here materialized view)
SELECT * FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY());

