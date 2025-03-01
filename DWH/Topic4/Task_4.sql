--TASK 1 – TABLE WITHOUT INDEX


--Create table test_index:
CREATE TABLE labs.test_index_plan (
num float NOT NULL,
load_date timestamptz NOT NULL
);
select * from labs.test_index_plan;

--Fill the table with a lot of test data:
INSERT INTO labs.test_index_plan(num, load_date)
SELECT random(), x
FROM generate_series('2017-01-01 0:00'::timestamptz, '2021-12-31 23:59:59'::timestamptz, '10 seconds'::interval) x;


--Check the plan of the select (twice at least, is any difference in plans?). Disable the parallel query planning If it needed:
SET max_parallel_workers_per_gather = 0;  -- set not run parallel process for query

--use explain
explain SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--use explain analyze
explain analyze SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--use explain analyze, buffers
explain (analyze, buffers)
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;


--Task2 - adding index

--set labs schema
set search_path to labs;

---------------------------------------------------
--Create B-Tree Index on test_index_plan table for load_date column.
CREATE INDEX test_index_load_date_idx ON labs.test_index_plan (load_date);

--disable parallel query planning:
SET max_parallel_workers_per_gather = 0;

--
explain
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;


--
explain analyze
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--
explain (analyze, buffers)
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--------------
-- Use INDEX ONLY SCAN method;
CREATE INDEX idx_load_date_num ON labs.test_index_plan (load_date, num);

--
explain
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;


--
explain analyze
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--
explain (analyze, buffers)
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
--ORDER BY 1;

--drop b-tree indexes
drop index labs.idx_load_date_num;
drop index labs.test_index_load_date_idx;

-------------------------------------------
--create BRIN index on test_index_plan table for load_date column.

--Check the plan of the select (twice at least, is any difference in plans?). Disable the parallel query planning If it needed:
SET max_parallel_workers_per_gather = 0;  -- set not run parallel process for query


CREATE INDEX idx_brin_load_date ON labs.test_index_plan using brin (load_date);

--
explain
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--
explain analyze
SELECT *
FROM labs.test_index_plan
WHERE load_dateBETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

--
explain (analyze, buffers)
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
--ORDER BY 1;

drop index labs.idx_brin_load_date;


--TASK 3 BULK INSERT

--Create new table:
CREATE TABLE labs.test_inserts (
num float NOT NULL,
load_date timestamptz NOT NULL
);

--Add B-Tree index on the table test_inserts on load_date column.
create index idx_brin_test_inserts on labs.test_inserts using brin (load_date);


--INSERT into test_inserts by using:
explain (analyze, buffers) SELECT num, load_date
FROM labs.test_index_plan;

select * from labs.test_inserts; 

--Create new table*:
CREATE TABLE labs.emp (
empno NUMERIC(4) NOT NULL CONSTRAINT emp_pk PRIMARY KEY,
ename VARCHAR(10) UNIQUE,
job VARCHAR(9),
mgr NUMERIC(4),
hiredate DATE
);

--Rewrite INSERT statements to more efficient way, run it:
INSERT INTO emp VALUES (1,'SMITH','CLERK',13,'17-DEC-80');
INSERT INTO emp VALUES (2,'ALLEN','SALESMAN',6,'20-FEB-81');
INSERT INTO emp VALUES (3,'WARD','SALESMAN',6,'22-FEB-81');
INSERT INTO emp VALUES (4,'JONES','MANAGER',9,'02-APR-81');
INSERT INTO emp VALUES (5,'MARTIN','SALESMAN',6,'28-SEP-81');
INSERT INTO emp VALUES (6,'BLAKE','MANAGER',9,'01-MAY-81');
INSERT INTO emp VALUES (7,'CLARK','MANAGER',9,'09-JUN-81');
INSERT INTO emp VALUES (8,'SCOTT','ANALYST',4,'19-APR-87');
INSERT INTO emp VALUES (9,'KING','PRESIDENT',NULL,'17-NOV-81');
INSERT INTO emp VALUES (10,'TURNER','SALESMAN',6,'08-SEP-81');
INSERT INTO emp VALUES (11,'ADAMS','CLERK',8,'23-MAY-87');
INSERT INTO emp VALUES (12,'JAMES','CLERK',6,'03-DEC-81');
INSERT INTO emp VALUES (13,'FORD','ANALYST',4,'03-DEC-81');
INSERT INTO emp VALUES (14,'MILLER','CLERK',7,'23-JAN-82');


insert into labs.emp (empno, ename, job,mgr, hiredate)
values 
(1, 'SMITH', 'CLERK', 13, '17-DEC-80'),
(2, 'ALLEN', 'SALESMAN', 6, '20-FEB-81'),
(3, 'WARD', 'SALESMAN', 6, '22-FEB-81'),
(4, 'JONES', 'MANAGER', 9, '02-APR-81'),
(5, 'MARTIN', 'SALESMAN', 6, '28-SEP-81'),
(6, 'BLAKE', 'MANAGER', 9, '01-MAY-81'),
(7, 'CLARK', 'MANAGER', 9, '09-JUN-81'),
(8, 'SCOTT', 'ANALYST', 4, '19-APR-87'),
(9, 'KING', 'PRESIDENT', NULL, '17-NOV-81'),
(10, 'TURNER', 'SALESMAN', 6, '08-SEP-81'),
(11, 'ADAMS', 'CLERK', 8, '23-MAY-87'),
(12, 'JAMES', 'CLERK', 6, '03-DEC-81'),
(13, 'FORD', 'ANALYST', 4, '03-DEC-81'),
(14, 'MILLER', 'CLERK', 7, '23-JAN-82');

select * from labs.emp;
drop table labs.emp;

--we can do this with with copy command
COPY labs.emp (empno, ename, job, mgr, hiredate)
FROM 'C:/Program Files/PostgreSQL/17/data/EPAM_study/emp_data.csv'
DELIMITER ','
CSV HEADER;


--TASK 4 COPY COMMAND

--Use COPY Command to export your test_index_plan table into csv file:
COPY labs.test_index_plan TO
'C:\Program Files\PostgreSQL\17\data\EPAM_study\test_index_plan.csv' DELIMITER ',' CSV HEADER;

--Change command to export column load_date from test_index_plan with quotes and num column without quotes.
COPY (select num, '"' || load_date || '"' from labs.test_index_plan) TO
'C:\Program Files\PostgreSQL\17\data\EPAM_study\test_index_plan.csv' DELIMITER ',' CSV HEADER;

--Use COPY to export data from test_index_plan table into csv file ‘test_index_plan_short.csv’
--where load_date between '2021-09-01 0:00' AND '2021-09-01 11:59:59'

COPY (
	select num, load_date
	from labs.test_index_plan 
	where load_date between '2021-09-01 0:00' AND '2021-09-01 11:59:59') TO
'C:\Program Files\PostgreSQL\17\data\EPAM_study\test_index_plan_short.csv'
DELIMITER ',' CSV HEADER;


--Create new table:
CREATE TABLE labs.test_copy (
num float NOT NULL,
load_date timestamptz NOT NULL
);

--Add B-Tree index on the table test_inserts on load_date column.
create index idx_test_inserts on labs.test_inserts (load_date);

--COPY into test_copy by using test_index_plan.csv file.

copy labs.test_copy (num, load_date)
from 'C:\Program Files\PostgreSQL\17\data\EPAM_study\test_index_plan.csv'
DELIMITER ',' CSV HEADER;


--TASK 5 UPSERT

--Add into emp table following information in one UPSERT statement:
INSERT INTO labs.emp (empno, ename, job, mgr, hiredate)
values
(1, 'SMITH', 'MANAGER', 13, '01-DEC-21'),
(14, 'KELLY', 'CLERK', 1, '01-DEC-21'),
(15, 'HANNAH', 'CLERK', 1, '01-DEC-21'),
(11, 'ADAMS', 'SALESMAN', 8, '01-DEC-21'),
(4, 'JONES', 'ANALIST', 9, '01-DEC-21')
ON CONFLICT (empno) 
DO UPDATE SET ename = EXCLUDED.ename, job = EXCLUDED.job, mgr = EXCLUDED.mgr, hiredate = EXCLUDED.hiredate;









