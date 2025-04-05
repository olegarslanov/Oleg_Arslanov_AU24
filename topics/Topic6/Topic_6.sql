--TASK 1: USE INHERITANCE (on old versions of Postgre)

--set schema labs for all tables
set search_path to labs;

--1. Create table:
CREATE TABLE SALES_INFO (
	id INTEGER,
	category VARCHAR(1),
	ischeck BOOLEAN,
	eventdate DATE
);

--2. create children tables with inheritance
create table sales_info_2022 (check (eventdate >= '2022-01-01' and eventdate < '2023-01-01')) inherits (sales_info);
create table sales_info_2023 (check (eventdate >= ' 2023-01-01 ' and eventdate < '2024-01-01')) inherits (sales_info);
create table sales_info_2024 (check (eventdate >= '2024-01-01' and eventdate < '2025-01-01')) inherits (sales_info);
create table sales_info_2025 (check (eventdate >= '2025-01-01' and eventdate < '2026-01-01')) inherits (sales_info);
--drop table sales_info_2021;

--3. Create partition function for children tables with inheritance
CREATE OR REPLACE FUNCTION partition_sales_info() RETURNS trigger
as $$
BEGIN
IF (new.eventdate >= '2025-01-01'::DATE AND new.eventdate < '2026-01-01'::DATE) THEN
	INSERT INTO sales_info_2025 VALUES (new.*) ;
ELSEIF (new.eventdate >= '2022-01-01'::DATE AND new.eventdate < '2023-01-01'::DATE) then
	INSERT INTO sales_info_2022 VALUES (new.*) ;
ELSEIF (new.eventdate >= '2023-01-01'::DATE AND new.eventdate < '2024-01-01'::DATE) then
	INSERT INTO sales_info_2023 VALUES (new.*) ;
ELSEIF (new.eventdate >= '2024-01-01'::DATE AND new.eventdate < '2025-01-01'::DATE) then
	INSERT INTO sales_info_2024 VALUES (new.*) ;
ELSE
RAISE EXCEPTION 'Out of range';
END IF;
RETURN NULL;
END;
$$ language plpgsql;
--drop function partition_sales_info cascade;

--4. Create trigger for your function and tables (this is automatization before we insert something in sales_info table):
CREATE TRIGGER partition_sales_info_trigger            --create trigger
BEFORE INSERT ON sales_info                            -- in table
FOR EACH ROW EXECUTE PROCEDURE partition_sales_info();  --starts function for move row to some table

--5. Generate test data and insert in SALES_INFO table:
INSERT INTO SALES_INFO(id,category, ischeck, eventDate)
SELECT id,
	('{"A","B","C","D","E","F","J","H","I","J","K"}'::text[])[((RANDOM())*10)::INTEGER] as category, --takes random letter from array
	((1*(RANDOM())::INTEGER)<1) as ischeck, --gives true or False
	(NOW() - '10 day'::INTERVAL * (RANDOM()::int * 100))::DATE as EventDate -- random date 
FROM generate_series(1,10000000) as id;

--6. Update some rows in SALES_INFO and set another eventdate
UPDATE SALES_INFO
SET eventdate = eventdate - INTERVAL '10 days'
WHERE id % 100000 = 0; --update only rows 100000 % 100000 = 0, 200000 % 100000 = 0 and same

--7. explain analyze fot sales_info table (with partitions)

explain analyze select * from sales_info;
explain analyze select * from sales_info where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info;

--8. Create table SALES_INFO_SIMPLE with the same structure as SALES_INFO but without
--partitioning. Insert test data from the 5th step. Compare plans of different queries:

CREATE TABLE SALES_INFO_SIMPLE as table sales_info;
INSERT INTO SALES_INFO_SIMPLE
select * from sales_info;
--drop table sales_info_simple;

--9. explain analyze fot sales_info table (without partitions)
explain analyze select * from sales_info_simple;
explain analyze select * from sales_info_simple where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info_simple where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info_simple;

--10. Delete one of partition (the oldest one). Create some general table like sales_info_3000 with
--the same structure as sales_info and add it as new partition.

DROP TABLE SALES_INFO_2022;

CREATE TABLE SALES_INFO_3000 (CHECK (eventdate >= '3000-01-01' AND eventdate < '3001-01-01')) INHERITS (SALES_INFO);

CREATE OR REPLACE FUNCTION partition_sales_info()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.eventdate >= '2022-01-01' AND NEW.eventdate < '2023-01-01') THEN
        INSERT INTO SALES_INFO_2022 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2023-01-01' AND NEW.eventdate < '2024-01-01') THEN
        INSERT INTO SALES_INFO_2023 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2024-01-01' AND NEW.eventdate < '2025-01-01') THEN
        INSERT INTO SALES_INFO_2024 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '3000-01-01' AND NEW.eventdate < '3001-01-01') THEN
        INSERT INTO SALES_INFO_3000 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Out of range';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--test inserting
INSERT INTO sales_info (id, category, ischeck, eventdate)
VALUES (1, 'A', TRUE, '3000-05-15');
SELECT * FROM sales_info_3000;
EXPLAIN ANALYZE 
SELECT * FROM sales_info 
WHERE eventdate = '2023-06-15';
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'sales_info_2023';


EXPLAIN ANALYZE SELECT * FROM sales_info WHERE eventdate = '2023-06-15';
SHOW constraint_exclusion;


------------------------------------------------------------------------------------------
--TASK2: USE DECLARATIVE PARTITIONING
------------------------------------------------------------------------------------------------------------
--Create table SALES_INFO_DP with structure and make it partitioned by eventdate:
create table sales_info_dp (
	id INTEGER not null,
	category VARCHAR(1),
	ischeck BOOLEAN,
	eventdate DATE not null
) partition by range (eventdate);
--partitions created by eventdate
--drop table sales_info_dp;

--2. Create 4-5 child tables with partitioning by eventdate column. One partition is one year. Each
--child table should be partitioned by list on category column. Use 2 lists of values and one
--default partition here. As a result you should have SALES_INFO_DP table with composite
--partitioning by range and list:

--create partitions by eventdate category:
CREATE TABLE SALES_INFO_DP_2022 PARTITION OF SALES_INFO_DP
FOR VALUES FROM ('2022-01-01') TO ('2023-01-01')
PARTITION BY LIST (category);

CREATE TABLE SALES_INFO_DP_2023 PARTITION OF SALES_INFO_DP
FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
PARTITION BY LIST (category);

CREATE TABLE SALES_INFO_DP_2024 PARTITION OF SALES_INFO_DP
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
PARTITION BY LIST (category);

CREATE TABLE SALES_INFO_DP_2025 PARTITION OF SALES_INFO_DP
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
PARTITION BY LIST (category);


-- create subpartitions by category:
CREATE TABLE SALES_INFO_DP_2022_GRP1 PARTITION OF SALES_INFO_DP_2022
FOR VALUES IN ('A', 'B', 'C', 'D', 'E');

CREATE TABLE SALES_INFO_DP_2022_GRP2 PARTITION OF SALES_INFO_DP_2022
FOR VALUES IN ('F', 'G', 'H', 'I', 'J');

CREATE TABLE SALES_INFO_DP_2022_DEFAULT PARTITION OF SALES_INFO_DP_2022
DEFAULT;

CREATE TABLE SALES_INFO_DP_2023_GRP1 PARTITION OF SALES_INFO_DP_2023
FOR VALUES IN ('A', 'B', 'C', 'D', 'E');

CREATE TABLE SALES_INFO_DP_2023_GRP2 PARTITION OF SALES_INFO_DP_2023
FOR VALUES IN ('F', 'G', 'H', 'I', 'J');

CREATE TABLE SALES_INFO_DP_2023_DEFAULT PARTITION OF SALES_INFO_DP_2023
DEFAULT;

CREATE TABLE SALES_INFO_DP_2024_GRP1 PARTITION OF SALES_INFO_DP_2024
FOR VALUES IN ('A', 'B', 'C', 'D', 'E');

CREATE TABLE SALES_INFO_DP_2024_GRP2 PARTITION OF SALES_INFO_DP_2024
FOR VALUES IN ('F', 'G', 'H', 'I', 'J');

CREATE TABLE SALES_INFO_DP_2024_DEFAULT PARTITION OF SALES_INFO_DP_2024
DEFAULT;

CREATE TABLE SALES_INFO_DP_2025_GRP1 PARTITION OF SALES_INFO_DP_2025
FOR VALUES IN ('A', 'B', 'C', 'D', 'E');

CREATE TABLE SALES_INFO_DP_2025_GRP2 PARTITION OF SALES_INFO_DP_2025
FOR VALUES IN ('F', 'G', 'H', 'I', 'J');

CREATE TABLE SALES_INFO_DP_2025_DEFAULT PARTITION OF SALES_INFO_DP_2025
DEFAULT;

--3. Add date to partitioned table:
INSERT INTO SALES_INFO_DP(id,category, ischeck, EventDate)
select
	id,
	('{"A","B","C","D","E","F","J","H","I","J","K"}'::text[])[((RANDOM())*10)::INTEGER] category,
	((1*(RANDOM())::INTEGER)<1) ischeck,
	(NOW() - '10 day'::INTERVAL * (RANDOM()::int * 100))::DATE EventDate
FROM generate_series(1,10000000) id;

--4. Update some rows in SALES_INFO_DP and set another category
UPDATE SALES_INFO_DP
SET category = 'Z'
WHERE id % 100000 = 0;

--------------------------------------------------------------------------------------------------------------
--5. Compare plans of different queries for tables SALES_INFO_DP and SALES_INFO_SIMPLE:

explain analyze select * from sales_info_dp;
explain analyze select * from sales_info_dp where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info_dp where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info_dp;

-- choose one category
EXPLAIN ANALYZE SELECT * FROM SALES_INFO_DP WHERE category = 'A';
EXPLAIN ANALYZE SELECT * FROM SALES_INFO_SIMPLE WHERE category = 'A';

-- choose list of category
EXPLAIN ANALYZE SELECT * FROM SALES_INFO_DP WHERE category IN ('A', 'B', 'C');
EXPLAIN ANALYZE SELECT * FROM SALES_INFO_SIMPLE WHERE category IN ('A', 'B', 'C');

--choose list of category in one day
EXPLAIN ANALYZE SELECT * FROM SALES_INFO_DP WHERE eventdate = '2023-06-15' AND category IN ('A', 'B', 'C');
EXPLAIN ANALYZE SELECT * FROM SALES_INFO_SIMPLE WHERE eventdate = '2023-06-15' AND category IN ('A', 'B', 'C');




--6. For one of the child tables with range partition by eventdate split one list partition for two.
ALTER TABLE SALES_INFO_DP_2022 DETACH PARTITION SALES_INFO_DP_2022_GRP1; -- remove relation (partition)from parent table
DROP TABLE SALES_INFO_DP_2022_GRP1;                                      -- so now we can drop table

CREATE TABLE SALES_INFO_DP_2022_GRP1_A_B_C PARTITION OF SALES_INFO_DP_2022
FOR VALUES IN ('A', 'B', 'C');

CREATE TABLE SALES_INFO_DP_2022_GRP1_D_E PARTITION OF SALES_INFO_DP_2022
FOR VALUES IN ('D', 'E');

--Return partition ('A','B','C','D','E'). Drop newly created (for ('A','B','C') and ('D','E'))
alter table sales_info_dp_2022 detach partition sales_info_dp_2022_grp1_a_b_c;
DROP TABLE SALES_INFO_DP_2022_GRP1_A_B_C; 
alter table sales_info_dp_2022 detach partition sales_info_dp_2022_grp1_d_e ;
DROP TABLE SALES_INFO_DP_2022_GRP1_D_E; 

CREATE TABLE SALES_INFO_DP_2022_GRP1 PARTITION OF SALES_INFO_DP_2022
FOR VALUES IN ('A', 'B', 'C', 'D', 'E');


CREATE INDEX sales_info_simple_idx ON SALES_INFO_SIMPLE (eventdate, category); 
drop index sales_info_simple_idx;

--TASK 3: USE PARALLEL QUERING

--1. Add parallel workers:
set max_parallel_workers_per_gather=4;

--2. Analyze plans for tables SALES_INFO, SALES_INFO_DP and SALES_INFO_SIMPLE by querying:

--explain analyze fot sales_info table (with partitions)
explain analyze select * from sales_info;
explain analyze select * from sales_info where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info;

--explain analyze fot sales_info_simple table (without partitions)
explain analyze select * from sales_info_simple;
explain analyze select * from sales_info_simple where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info_simple where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info_simple;

--explain analyze fot sales_info_dp table
explain analyze select * from sales_info_dp;
explain analyze select * from sales_info_dp where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info_dp where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info_dp;


--3. Add indexes on any of table with partitions. Check how plans are change.

CREATE INDEX sales_info_dp_idx ON SALES_INFO_DP (eventdate, category);   -- with partitioning
drop index sales_info_dp_idx;

explain analyze select * from sales_info_dp;
explain analyze select * from sales_info_dp where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info_dp where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info_dp;


set max_parallel_workers_per_gather=1;

CREATE INDEX sales_info_idx ON SALES_INFO (eventdate, category);   -- with partitioning
drop index sales_info_idx;

explain analyze select * from sales_info;
explain analyze select * from sales_info where eventdate between '2023-01-01' and '2024-01-01';
explain analyze select * from sales_info where eventdate = '2024-06-05';
explain analyze select count (*) from sales_info;




