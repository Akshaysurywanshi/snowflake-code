-- ==============
-- Data Sampling
-- ==============

-- Examples:
1. select * from tablename sample row(10);
-- Return a sample with 10% of rows

2. select * from tablename tablesample block (20);
-- Return a sample with data from 20% blocks

3. select * from tablename sample system (10) seed (111) ;
-- Return a sample with data from 10% of blocks and guarantees same data set if we use seed 111 next time.

4. select * from tablename tablesample (100);
-- Return an entire table, including all rows into the sample


5. select * from tablename sample row (0);
-- Return an empty sample

6. select * from tablename sample (10 rows);
-- Return a fixed-size sample of 10 rows