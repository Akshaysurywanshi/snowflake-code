Alter storage integration s3_int
set STORAGE_ALLOWED_LOCATIONS = ('azure://xml.blob.core.windows.net/data/books.xml');

DESC integration s3_int;

--create required database and schemas
// Create required datbase and schemas
create database if not exists my_db
create schema if not exists my_db.file_formats;
create schema if not exists my_db.external_stages;
create schema if not exists my_db.stage_tbls;
create schema if not exists my_db.integration_tbls;

// Create file format object for xml files
CREATE OR REPLACE file format mydb.file_formats.xml_fileformat
    type = xml;



-- // Create stage on external s3 location
CREATE OR REPLACE STAGE mydb.external_stages.aws_s3_xml
    URL = 'azure://xml.blob.core.windows.net/data/books.xml'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = mydb.file_formats.xml_fileformat ;

// Listing files under your s3 xml bucket
list @mydb.external_stages.aws_s3_xml;


// View data from xml file
select * from @mydb.external_stages.aws_s3_xml;


// Create variant table to load xml file
CREATE OR REPLACE TABLE mydb.stage_tbls.STG_BOOKS(xml_data variant);



// Load xml file to variant table
copy into mydb.stage_tbls.STG_BOOKS
from @mydb.external_stages.aws_s3_xml
force=TRUE;

// Query stage table
select * from mydb.stage_tbls.STG_BOOKS;


-- // Create Target table to load final data
CREATE OR REPLACE TABLE mydb.integration_tbls.BOOKS
( 
 title varchar(50),
 author varchar(50),
 year varchar(20),
 price number(10,2)
);
select * from mydb.integration_tbls.BOOKS

-- // To get the root element name
select xml_data:"@" from mydb.stage_tbls.STG_BOOKS;

-- // To get root element value
select xml_data:"$" from mydb.stage_tbls.STG_BOOKS;



-- // Fetch actual data from file
SELECT 
XMLGET(bk.value, 'title' ):"$" as "title",
XMLGET(bk.value, 'author' ):"$" as "author"
FROM mydb.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;

// Fetch data and assign datatypes
SELECT 
XMLGET(bk.value, 'title' ):"$" :: varchar as "title",
XMLGET(bk.value, 'author' ):"$" :: varchar as "author",
XMLGET(bk.value, 'year' ):"$" :: varchar as "year",
XMLGET(bk.value, 'price' ):"$" :: number(10,2) as "price"

FROM mydb.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;


// Insert data from stage table to final target table
INSERT INTO mydb.integration_tbls.BOOKS
SELECT 
XMLGET(bk.value, 'title' ):"$" :: varchar as "title",
XMLGET(bk.value, 'author' ):"$" :: varchar as "author",
XMLGET(bk.value, 'year' ):"$" :: varchar as "year",
XMLGET(bk.value, 'price' ):"$" :: number(10,2) as "price",
FROM mydb.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;


// View final data
SELECT * FROM mydb.integration_tbls.BOOKS;