-- ==================
-- Stored Procedures 
-- ==================
// Create database and schemas if not exists
CREATE DATABASE IF NOT EXISTS MYOWN_DB
use database MYOWN_DB;
CREATE SCHEMA IF NOT EXISTS MYPROCS;

==============
SQL Procudure
==============
// Setup some sample data
CREATE OR REPLACE TABLE MYOWN_DB.PUBLIC.CUST_SALES
(CID INT, CNAME VARCHAR, PRODNAME VARCHAR, PROD_CAT VARCHAR, PRICE NUMBER);

INSERT INTO MYOWN_DB.PUBLIC.CUST_SALES VALUES
(101, 'RAMU', 'REFRIGERATOR', 'ELECTRONICS', 25000 ),
(101, 'RAMU', 'TV', 'ELECTRONICS' , 33500),
(101, 'RAMU', 'MAGGIE', 'FOOD', 200),
(102, 'LATHA', 'T-SHIRT', 'FASHION', 1099),
(102, 'LATHA', 'JEANS', 'FASHION', 2999),
(102, 'LATHA', 'WALLNUTS', 'FOOD', 2000),
(102, 'LATHA', 'WASHING MACHINE', 'ELECTRONICS', 29000),
(102, 'LATHA', 'SMART WATCH', 'ELECTRONICS', 12000),
(102, 'LATHA', 'ALMOND', 'FOOD', 1500),
(102, 'LATHA', 'TOUR DAL', 'FOOD', 500),
(102, 'LATHA', 'RICE', 'FOOD', 1300);

select * from MYOWN_DB.PUBLIC.CUST_SALES;




// Create a procedure to calculate total amount spent in a store

CREATE OR REPLACE PROCEDURE MYPROCS.CUST_TOT_PRICE(ID INT, CAT VARCHAR)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
declare
total_spent float;
cur1 cursor for select CID, PROD_CAT, PRICE from MYOWN_DB.PUBLIC.CUST_SALES;

begin
total_spent := 0;

for rec in cur1
do
    if (rec.PROD_CAT = :CAT and rec.CID = :ID) then 
        total_spent := total_spent+rec.PRICE;
    end if;
end for;

return total_spent;
end;

$$

// Calls to the stored procedure
CALL MYPROCS.CUST_TOT_PRICE(101, 'ELECTRONICS');
CALL MYPROCS.CUST_TOT_PRICE(102, 'FOOD');
CALL MYPROCS.CUST_TOT_PRICE(102, 'ELECTRONICS');
CALL MYPROCS.CUST_TOT_PRICE(103, 'FASHION');








-- ======================
-- Java Script Procudure
-- ======================
-- firststep is sql query put it in variable like below then we have to create executable statement based out of this variable,(snowflake.createStatement() in that function write sql query)

/* How to Execute a sql query from java script
var my_sql_command1 = "delete from history_table where event_year less_than_Symbol 2016";
var statement1 = snowflake.CreateStatement(sqlText: my_sql_command1);
statement1.execute();
*/

// create a procedure to get the row count of a table
create or replace procedure get_row_count(table_name VARCHAR)
  returns float not null
  language javascript
as
$$
  var row_count = 0;
  
  var sql_command = "select count(*) from " + TABLE_NAME;
  
  var stmt = snowflake.createStatement(
         { sqlText: sql_command }
      );
  var res = stmt.execute();  
  
  res.next();
  
  row_count = res.getColumnValue(1);
  
  return row_count;
  
$$
;

// Call the procedure 
call GET_ROW_COUNT('MYOWN_DB.PUBLIC.CUST_SALES');