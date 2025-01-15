--Task 2

--Operations report
--____________________________________________________________________________________________
--|                    | --Units| --create | -- delete(every 3 row)| --vacuum full | truncate |
--|____________________|________|__________|_______________________|_______________|__________|                      
--| space consumption  | --MB   | --575    |      --575            | --383         | --0.01   |
--|____________________|________|__________|_______________________|_______________|__________|
--| operation duration | --sec  | --38     |      --25             | --12          | --1      |
--|____________________|________|__________|_______________________|_______________|__________|


--After creating the table, it occupies 575 MB on disk (~17.4K rows/MB), and the creation time is 38 seconds (~260K rows/second). After
--deleting every third row, the database still occupies the same 575 MB, taking 25 seconds. This means that space is not returned
--to the operating system. However, after using the VACUUM FULL command, the space is reduced to 383 MB in 12 seconds. Using the TRUNCATE
--command effectively reduces the space occupied by the table to nearly 0 MB, and this operation takes only 1 second. The TRUNCATE operation
--instantaneously deletes all rows from the table

--Main Conclusion: The TRUNCATE command is significantly more efficient than deleting rows one by one, as it frees up space almost immediately,
--whereas other operations do not return space to the operating system until a VACUUM is performed.











--Note: 
--Make sure to turn autocommit on in connection settings before attempting the following tasks. Otherwise you might get an error at some
--point.


--1. Create table ‘table_to_delete’ and fill it with the following query:

               --CREATE TABLE table_to_delete AS
               --SELECT 'veeeeeeery_long_string' || x AS col
               --FROM generate_series(1,(10^7)::int) x; -- generate_series() creates 10^7 rows of sequential numbers from 1 to 10000000 (10^7)

CREATE TABLE table_to_delete AS
SELECT 'veeeeeeery_long_string' || x AS col
FROM generate_series(1,(10^7)::int) x;
-- this code generates new table with 10 000 000 rows and it takes 38 seconds

--insert into table_to_delete
--SELECT 'veeeeeeery_long_string' || x AS col
--FROM generate_series(1,(10^7)::int) x;


--2. Lookup how much space this table consumes with the following query:


               --SELECT *, pg_size_pretty(total_bytes) AS total,
                                    --pg_size_pretty(index_bytes) AS INDEX,
                                    --pg_size_pretty(toast_bytes) AS toast,
                                    --pg_size_pretty(table_bytes) AS TABLE
               --FROM ( SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
                               --FROM (SELECT c.oid,nspname AS table_schema,
                                                               --relname AS TABLE_NAME,
                                                              --c.reltuples AS row_estimate,
                                                              --pg_total_relation_size(c.oid) AS total_bytes,
                                                              --pg_indexes_size(c.oid) AS index_bytes,
                                                              --pg_total_relation_size(reltoastrelid) AS toast_bytes
                                              --FROM pg_class c
                                              --LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                                              --WHERE relkind = 'r'
                                              --) a
                                    --) a
               --WHERE table_name LIKE '%table_to_delete%';

SELECT 
	*, 
	pg_size_pretty(total_bytes) AS total,
    pg_size_pretty(index_bytes) AS INDEX,
    pg_size_pretty(toast_bytes) AS toast,
    pg_size_pretty(table_bytes) AS TABLE
FROM (
	SELECT 
		*,
		total_bytes - index_bytes - COALESCE(toast_bytes,0) AS table_bytes
	FROM (
		SELECT 
			c.oid,
			nspname AS table_schema,
			relname AS TABLE_NAME,
			c.reltuples AS row_estimate,
			pg_total_relation_size(c.oid) AS total_bytes,
			pg_indexes_size(c.oid) AS index_bytes,
			pg_total_relation_size(reltoastrelid) AS toast_bytes
        FROM pg_class c
      	LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE relkind = 'r'
     ) a
) a
WHERE table_name LIKE '%table_to_delete%';

-- this table consumes 575 MB (about 20K rows ~ 1MB)


--3. Issue the following DELETE operation on ‘table_to_delete’:

               --DELETE FROM table_to_delete
               --WHERE REPLACE(col, 'veeeeeeery_long_string','')::int % 3 = 0; -- removes 1/3 of all rows

      --a) Note how much time it takes to perform this DELETE statement;   25 seconds
      --b) Lookup how much space this table consumes after previous DELETE;  575 MB
      --c) Perform the following command (if you're using DBeaver, press Ctrl+Shift+O to observe server output (VACUUM results)): 
               --VACUUM FULL VERBOSE table_to_delete;  
      --d) Check space consumption of the table once again and make conclusions;     383 MB
      --e) Recreate ‘table_to_delete’ table;


DELETE FROM table_to_delete
WHERE REPLACE(col, 'veeeeeeery_long_string','')::int % 3 = 0;

VACUUM FULL VERBOSE table_to_delete;

--Recreate table table_to_delete
create table temp_table as
select distinct * from table_to_delete;

truncate table table_to_delete;

insert into table_to_delete 
select * from temp_table;

drop table temp_table;


--4. Issue the following TRUNCATE operation:

               --TRUNCATE table_to_delete;
      --a) Note how much time it takes to perform this TRUNCATE statement.   1 seconds 
      --b) Compare with previous results and make conclusion.   
      --c) Check space consumption of the table once again and make conclusions;   0.01 MB

TRUNCATE table_to_delete;     

--5. Hand over your investigation's results to your trainer. The results must include:

      --a) Space consumption of ‘table_to_delete’ table before and after each operation;
      --b) Duration of each operation (DELETE, TRUNCATE)
