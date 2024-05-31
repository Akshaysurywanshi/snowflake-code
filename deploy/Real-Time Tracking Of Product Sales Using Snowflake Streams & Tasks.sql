USE ROLE sysadmin;
USE DATABASE demo_db;
USE SCHEMA public;
USE WAREHOUSE compute_wh;



-- Creating The Raw & Final Tables
--create a raw table where change data capture will be triggered
CREATE OR REPLACE TABLE sales_raw_tbl (
  product_id NUMBER,
  sale_date DATE,
  units_sold NUMBER
);

--kjhkjg



--create the final table where post cdc, data will 
CREATE OR REPLACE TABLE sales_final_tbl (
  product_id NUMBER,
  sale_date DATE,
  units_sold NUMBER
);
-- We’ll use this script to create 2 tables: ‘sales_raw_tbl’ and ‘sales_final_tbl’, 
-- The ‘sales_raw_tbl’ will capture the real-time sales data while ‘sales_final_tbl’ will hold the processed data.



-- Creating The Stream
CREATE OR REPLACE STREAM   
sales_stream ON TABLE sales_raw_tbl
APPEND_ONLY=TRUE;
-- With this script, we have created a stream called ‘sales_stream’, on ‘sales_raw_tbl’,. 
-- This stream will track any changes made to the ‘sales_raw_tbl’.


-- Loading Initial Data Into The Final Table
-- 1st time data load from sales_raw_tbl to sales_final_tbl
INSERT INTO sales_final_tbl SELECT * FROM sales_raw_tbl;
-- This script loads the initial data from ‘sales_raw_tbl’ into ‘sales_final_tbl’. 
-- This could represent an initial bulk load of historical sales data.


-- Creating The Task
--create a task to load these new CDC data to final table
CREATE OR REPLACE TASK sales_task
    WAREHOUSE = compute_wh 
    SCHEDULE  = '5 minute'
  WHEN
    SYSTEM$STREAM_HAS_DATA('sales_stream')
  AS
    INSERT INTO sales_final_tbl SELECT * FROM sales_stream;
-- We’ll create a task named ‘sales_task’ that uses the ‘compute_wh’ for its computation.
--  The task is scheduled to run every 5 minutes when there's new data in the ‘sales_stream’. 
-- The task's job is to execute the INSERT statement which updates the ‘sales_final_tbl’ based on the changes captured in the ‘sales_stream’.




-- Activating The Task
--activate the task to start consuming it
USE ROLE accountadmin;
ALTER TASK sales_task RESUME;
-- This activates the ‘sales_task’. Once activated, the task will start running according to its schedule,
--  ensuring that ‘sales_final_tbl’ is always up-to-date with the latest sales data.