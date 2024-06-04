MYDB.PUBLIC

//Create a table with no cluster keys
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_NONCLUSTER (
 C_CUSTKEY NUMBER(38,0),
 C_NAME VARCHAR(25),
 C_ADDRESS VARCHAR(40),
 C_NATIONKEY NUMBER(38,0),
 C_PHONE VARCHAR(15),
 C_ACCTBAL NUMBER(12,2),
 C_MKTSEGMENT VARCHAR(10),
 C_COMMENT VARCHAR(117)
);

// Insert data into above non-clustered table
INSERT INTO PUBLIC.CUSTOMER_NONCLUSTER
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;


//Create a table with cluster key
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_CLUSTER (
 C_CUSTKEY NUMBER(38,0),
 C_NAME VARCHAR(25),
 C_ADDRESS VARCHAR(40),
 C_NATIONKEY NUMBER(38,0),
 C_PHONE VARCHAR(15),
 C_ACCTBAL NUMBER(12,2),
 C_MKTSEGMENT VARCHAR(10),
 C_COMMENT VARCHAR(117)
 )cluster by(C_NATIONKEY);

// Insert data into above clustered table
INSERT INTO PUBLIC.CUSTOMER_CLUSTER
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;


// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.CUSTOMER_NONCLUSTER WHERE C_NATIONKEY=2; --  15 sec -- 420/420 mp scanned
SELECT * FROM PUBLIC.CUSTOMER_CLUSTER WHERE C_NATIONKEY=2; -- 7 sec -- 22/482 mp scanned


ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE PUBLIC.ORDERS_NONCLUSTER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

CREATE OR REPLACE TABLE PUBLIC.ORDERS_CLUSTER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;


// Add Cluster key to the table
ALTER TABLE PUBLIC.ORDERS_CLUSTER CLUSTER BY (YEAR(O_ORDERDATE));




// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.ORDERS_NONCLUSTER WHERE YEAR(O_ORDERDATE) = 1995; -- 15 sec -- 81/231 mps
SELECT * FROM PUBLIC.ORDERS_CLUSTER WHERE YEAR(O_ORDERDATE) = 1995; -- 15 sec -- 36/228 mps


// Alter Table to add multiple cluster keys
ALTER TABLE PUBLIC.ORDERS_CLUSTER CLUSTER BY (YEAR(O_ORDERDATE), O_ORDERPRIORITY);

//To look at clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('ORDERS_CLUSTER');

// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.ORDERS_NONCLUSTER WHERE YEAR(O_ORDERDATE) = 1996 and O_ORDERPRIORITY = '1-URGENT'; -- 4.2sec -- 71/231 
SELECT * FROM PUBLIC.ORDERS_CLUSTER WHERE YEAR(O_ORDERDATE) = 1996 and O_ORDERPRIORITY = '1-URGENT'; -- 3.8sec -- 12/239

