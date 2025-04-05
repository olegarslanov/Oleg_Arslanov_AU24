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

--Create B-Tree Index on test_index_plan table for load_date column.
CREATE INDEX test_index_load_date_idx ON test_index_plan (load_date);

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
ORDER BY 1;










