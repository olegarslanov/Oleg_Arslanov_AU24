--drop table user_dilab_student56.fct_taxi_trips;

--create table
CREATE TABLE IF NOT EXISTS user_dilab_student56.fct_taxi_trips (
    trip_sur_id          BIGINT PRIMARY KEY,  
    vendor_sur_id        BIGINT  NOT NULL,        
    customer_sur_id      BIGINT  not null,                 
    junk_sur_id          BIGINT  not null,                 
    rate_sur_id          BIGINT  not null,               
    promo_sur_id         BIGINT  not null,              
    pickup_location_id   BIGINT  not null,             
    dropoff_location_id  BIGINT  not null, 
    booking_date_id      BIGINT  , 
    pickup_date_id       BIGINT  ,             
    dropoff_date_id      BIGINT  , 
    payment_date_id      BIGINT  ,  
    pickup_time_id       BIGINT  ,            
    dropoff_time_id      BIGINT  , 
    booking_time_id      BIGINT  , 
    payment_time_id      BIGINT  ,
    trip_src_id          bigint  not null unique,
    trip_id              VARCHAR(50)  not null,
    trip_duration        INT           not null, 
    passenger_count      INT           not null, 
    distance_miles       DECIMAL(10,2) not null, 
    trip_amount          DECIMAL(10,2) not null,            
    update_dt            TIMESTAMP     not null, 
    insert_dt            TIMESTAMP     not null
)
DISTSTYLE KEY
DISTKEY (trip_sur_id) -- i am telling redshift to distribute rows across nodes based on this column
SORTKEY (trip_sur_id, trip_duration); -- physicaly sort and store data on virtual disk (where, order by is quicker)


--copy all data from S3 to this table
COPY user_dilab_student56.fct_taxi_trips
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/fct_taxi_trips/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
IGNOREHEADER 1;

--select * from user_dilab_student56.fct_taxi_trips;


-- DIM_DATES
CREATE TABLE if not exists user_dilab_student56.dim_dates (
    date_sur_id BIGINT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    day_of_week VARCHAR(255) NOT NULL,
    day integer NOT NULL,
    month integer NOT NULL,
    quarter integer NOT NULL,
    year integer NOT NULL
);

--copy all data from S3 to this table
COPY user_dilab_student56.dim_dates
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/dim_dates/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
IGNOREHEADER 1;

select * from user_dilab_student56.dim_dates;

-- DIM_TIME
CREATE TABLE if not exists user_dilab_student56.dim_time (
    time_sur_id BIGINT PRIMARY KEY,
    calendar_time TIME NOT NULL UNIQUE,
    hour integer NOT NULL,
    minute integer NOT NULL
);

--copy all data from S3 to this table
COPY user_dilab_student56.dim_time
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/dim_time/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
--DATEFORMAT AS 'MM-DD-YYYY'
IGNOREHEADER 1;

--select * from stl_load_errors;


--Super Analysis

--a) avg_fare and avg_duration
select 	
	avg(trip_duration) as avg_suration_sec,
	avg(trip_amount) avg_fare_usd,
	count(*) as total_trips
from user_dilab_student56.fct_taxi_trips;

--b) avg_fare and avg_duration by day of week
SELECT
  d.day_of_week,
  AVG(f.trip_duration) AS avg_duration,
  AVG(f.trip_amount)   AS avg_fare
FROM user_dilab_student56.fct_taxi_trips f
JOIN user_dilab_student56.dim_dates d
  ON f.pickup_date_id = d.date_sur_id
GROUP BY d.day_of_week;



--Analysis fct_taxi_trips
SELECT tablename, "column", encoding
FROM pg_table_def
WHERE schemaname = 'user_dilab_student56'
  AND tablename = 'fct_taxi_trips';

SELECT 
  "table", diststyle, sortkey1, sortkey_num
FROM svv_table_info
WHERE schema = 'user_dilab_student56'
  AND "table" = 'fct_taxi_trips';


--Analysis dim_date
SELECT tablename, "column", encoding
FROM pg_table_def
WHERE schemaname = 'user_dilab_student56'
  AND tablename = 'dim_dates';

SELECT 
  "table", diststyle, sortkey1, sortkey_num
FROM svv_table_info
WHERE schema = 'user_dilab_student56'
  AND "table" = 'dim_dates';


--Analysis dim_time
SELECT tablename, "column", encoding
FROM pg_table_def
WHERE schemaname = 'user_dilab_student56'
  AND tablename = 'dim_time';

SELECT 
  "table", diststyle, sortkey1, sortkey_num
FROM svv_table_info
WHERE schema = 'user_dilab_student56'
  AND "table" = 'dim_time';


-- looking for encode types
SELECT 
  tablename, 
  "column", 
  encoding
FROM pg_table_def
WHERE schemaname = 'user_dilab_student56'
  AND tablename = 'fct_taxi_trips_manual'; -- here no encoding
  
----------------------------------------------------------------------------------------------------------------------------------------  
--3 Task
  
--drop table user_dilab_student56.fct_taxi_trips_defaultcomp;
  
-- a. create new table with compression
CREATE TABLE user_dilab_student56.fct_taxi_trips_defaultcomp
(
    trip_sur_id          BIGINT,
    vendor_sur_id        BIGINT,
    customer_sur_id      BIGINT,
    junk_sur_id          BIGINT,
    rate_sur_id          BIGINT,
    promo_sur_id         BIGINT,
    pickup_location_id   BIGINT,
    dropoff_location_id  BIGINT,
    booking_date_id      BIGINT,
    pickup_date_id       BIGINT,
    dropoff_date_id      BIGINT,
    payment_date_id      BIGINT,
    pickup_time_id       BIGINT,
    dropoff_time_id      BIGINT,
    booking_time_id      BIGINT,
    payment_time_id      BIGINT,
    trip_src_id          BIGINT,
    trip_id              VARCHAR(50),
    trip_duration        INT,
    passenger_count      INT,
    distance_miles       DECIMAL(10,2),
    trip_amount          DECIMAL(10,2),
    update_dt            TIMESTAMP,
    insert_dt            TIMESTAMP
)
COMPOUND SORTKEY (trip_sur_id); 

COPY user_dilab_student56.fct_taxi_trips_defaultcomp
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/fct_taxi_trips/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
IGNOREHEADER 1
COMPUPDATE ON; -- command for using auto analyse and using auto encoding



-- b. without comression

CREATE TABLE user_dilab_student56.fct_taxi_trips_withoutcomp
(
    trip_sur_id          BIGINT encode raw,
    vendor_sur_id        BIGINT encode raw,
    customer_sur_id      BIGINT encode raw,
    junk_sur_id          BIGINT encode raw,
    rate_sur_id          BIGINT encode raw,
    promo_sur_id         BIGINT encode raw,
    pickup_location_id   BIGINT encode raw,
    dropoff_location_id  BIGINT encode raw,
    booking_date_id      BIGINT encode raw,
    pickup_date_id       BIGINT encode raw,
    dropoff_date_id      BIGINT encode raw,
    payment_date_id      BIGINT encode raw,
    pickup_time_id       BIGINT encode raw,
    dropoff_time_id      BIGINT encode raw,
    booking_time_id      BIGINT encode raw,
    payment_time_id      BIGINT encode raw,
    trip_src_id          BIGINT encode raw,
    trip_id              VARCHAR(50) encode raw,
    trip_duration        INT encode raw,
    passenger_count      INT encode raw,
    distance_miles       DECIMAL(10,2) encode raw,
    trip_amount          DECIMAL(10,2) encode raw,
    update_dt            TIMESTAMP encode raw,
    insert_dt            TIMESTAMP encode raw
)
COMPOUND SORTKEY (trip_sur_id); 

COPY user_dilab_student56.fct_taxi_trips_withoutcomp
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/fct_taxi_trips/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
IGNOREHEADER 1;


-- c. create new table with compression (same columns but applying recommended encoding types)]

--auto analyze for creating optimazing table
ANALYZE COMPRESSION user_dilab_student56.fct_taxi_trips_withoutcomp;


CREATE TABLE user_dilab_student56.fct_taxi_trips_analyzedcomp
(
    trip_sur_id          BIGINT,
    vendor_sur_id        BIGINT encode az64,
    customer_sur_id      BIGINT encode az64,
    junk_sur_id          BIGINT encode az64,
    rate_sur_id          BIGINT encode az64,
    promo_sur_id         BIGINT encode az64,
    pickup_location_id   BIGINT encode az64,
    dropoff_location_id  BIGINT encode az64,
    booking_date_id      BIGINT encode az64,
    pickup_date_id       BIGINT encode az64,
    dropoff_date_id      BIGINT encode az64,
    payment_date_id      BIGINT encode az64,
    pickup_time_id       BIGINT encode az64,
    dropoff_time_id      BIGINT encode az64,
    booking_time_id      BIGINT encode az64,
    payment_time_id      BIGINT encode az64,
    trip_src_id          BIGINT encode az64,
    trip_id              VARCHAR(50) encode zstd,
    trip_duration        INT encode delta,
    passenger_count      INT encode az64,
    distance_miles       DECIMAL(10,2) encode az64,
    trip_amount          DECIMAL(10,2) encode az64,
    update_dt            TIMESTAMP encode az64,
    insert_dt            TIMESTAMP encode az64
)
COMPOUND SORTKEY (trip_sur_id); 

COPY user_dilab_student56.fct_taxi_trips_analyzedcomp
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/fct_taxi_trips/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
IGNOREHEADER 1;


-- check what is size of tables with or without compression

SELECT 
  "table" AS table_name, 
  size AS size_mb,
  tbl_rows AS rows
FROM svv_table_info
WHERE schema = 'user_dilab_student56'
  AND "table" IN (
    'fct_taxi_trips_defaultcomp',
    'fct_taxi_trips_withoutcomp',
    'fct_taxi_trips_analyzedcomp'
  );


---------------------------------------------------------------------------------------------------------
-- 4. Optimization query (working with existing tables, dont change physically structure of tables ... changing SELECT)

-- a) optimaze procedure query (joining 3 tables)

-- turn off cash for quality test result (it is working directly from disk always)
SET enable_result_cache_for_session TO OFF;

--table for report (Redshift must have table for result)
CREATE TABLE IF NOT EXISTS user_dilab_student56.report_trip_data (
  calendar_date DATE,
  trip_duration INT,
  trip_amount DECIMAL(10,2)
);

--create simple procedure
CREATE OR REPLACE PROCEDURE user_dilab_student56.generate_trip_report()
AS $$
BEGIN
  DELETE FROM user_dilab_student56.report_trip_data;

  INSERT INTO user_dilab_student56.report_trip_data
  SELECT 
    d.calendar_date,
    SUM(t.trip_duration) AS total_duration,
    SUM(t.trip_amount) AS total_amount
  FROM user_dilab_student56.fct_taxi_trips t
  JOIN user_dilab_student56.dim_dates d 
    ON t.pickup_date_id = d.date_sur_id
  JOIN user_dilab_student56.dim_time tm 
    ON t.pickup_time_id = tm.time_sur_id
  WHERE d.year = 2016
  GROUP BY d.calendar_date
  ORDER BY d.calendar_date;
END;
$$ LANGUAGE plpgsql;


-- so for report first se need call procedure and then open table with results
call user_dilab_student56.generate_trip_report();
SELECT * FROM user_dilab_student56.report_trip_data;


--check execution plan before optimization
explain
SELECT 
    d.calendar_date,
    t.trip_duration,
    t.trip_amount
FROM user_dilab_student56.fct_taxi_trips t
JOIN user_dilab_student56.dim_dates d ON t.pickup_date_id = d.date_sur_id
JOIN user_dilab_student56.dim_time tm ON t.pickup_time_id = tm.time_sur_id
WHERE d.year = 2016;

-- get time of query before optimization
SELECT 
  query,
  starttime,
  endtime,
  DATEDIFF(ms, starttime, endtime) AS duration_ms,
  SUBSTRING(querytxt, 1, 100) AS preview
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%report_trip_report%'
ORDER BY starttime DESC
LIMIT 5;

-- 
--optimizyng query

DROP TABLE IF EXISTS user_dilab_student56.report_trip_data;

CREATE TABLE user_dilab_student56.report_trip_data (
  calendar_date DATE,
  trip_duration INT,
  trip_amount DECIMAL(10,2)
);

ANALYZE user_dilab_student56.fct_taxi_trips;
ANALYZE user_dilab_student56.dim_dates;
ANALYZE user_dilab_student56.dim_time;
ANALYZE user_dilab_student56.report_trip_data;


CALL user_dilab_student56.generate_trip_report();

-- get time needed fo query by stl_query
SELECT 
  query,
  starttime,
  endtime,
  DATEDIFF(ms, starttime, endtime) AS duration_ms,
  SUBSTRING(querytxt, 1, 80)
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%generate_trip_report%'
ORDER BY starttime DESC
LIMIT 10;

select * from stl_query;

----------------------------
--b) change joining one table because dim_time is small ... for see difference I adding customers table

-- DIM_CUSTOMERS_SCD
CREATE TABLE if not exists user_dilab_student56.dim_customers_scd (
    customer_sur_id BIGINT PRIMARY KEY,
    customer_type VARCHAR(255) NOT NULL,
    customer_telephone VARCHAR(255),
    customer_src_id                   bigint NOT null,          
    source_system VARCHAR(255) NOT NULL,
    source_entity VARCHAR(255) NOT NULL,
    start_dt TIMESTAMP NOT NULL,
    end_dt TIMESTAMP,
    is_active boolean NOT NULL,                         
    insert_dt TIMESTAMP NOT NULL
);

--copy all data from S3 to this table
COPY user_dilab_student56.dim_customers_scd
FROM 's3://student-oleg-arslanov-bucket/di_dwh_database/bl_dm/dim_customers_scd/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ',' 
CSV 
IGNOREHEADER 1;

select * from user_dilab_student56.dim_customers_scd;

-- create table with new parametrs
DROP TABLE IF EXISTS user_dilab_student56.report_trip_data;

CREATE TABLE user_dilab_student56.report_trip_data (
  calendar_date DATE,
  customer_type VARCHAR(50),
  trip_duration INT,
  trip_amount DECIMAL(10,2)
);

-- create procedure
CREATE OR REPLACE PROCEDURE user_dilab_student56.generate_trip_report()
AS $$
BEGIN
  DELETE FROM user_dilab_student56.report_trip_data;

  INSERT INTO user_dilab_student56.report_trip_data
  SELECT 
    d.calendar_date,
    c.customer_type,
    SUM(t.trip_duration) AS total_duration,
    SUM(t.trip_amount) AS total_amount
  FROM user_dilab_student56.fct_taxi_trips t
  JOIN user_dilab_student56.dim_dates d 
    ON t.pickup_date_id = d.date_sur_id
  JOIN user_dilab_student56.dim_customers_scd c 
    ON t.customer_sur_id = c.customer_sur_id
  WHERE d.year = 2016
  GROUP BY d.calendar_date, c.customer_type
  ORDER BY d.calendar_date, c.customer_type;
END;
$$ LANGUAGE plpgsql;

-- turn off cash for quality test result (it is working directly from disk always)
SET enable_result_cache_for_session TO OFF;

-- call procedure
CALL user_dilab_student56.generate_trip_report();

-- test time needed for query
SELECT starttime, endtime, DATEDIFF(ms, starttime, endtime) AS duration_ms
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%generate_trip_report%'
ORDER BY starttime DESC
LIMIT 5;


--using analyze and see how time is changing
ANALYZE user_dilab_student56.fct_taxi_trips;
ANALYZE user_dilab_student56.dim_dates;
ANALYZE user_dilab_student56.dim_customers_scd;
ANALYZE user_dilab_student56.report_trip_data;



-- call procedure
CALL user_dilab_student56.generate_trip_report();

-- test time needed for query
SELECT starttime, endtime, DATEDIFF(ms, starttime, endtime) AS duration_ms
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%generate_trip_report%'
ORDER BY starttime DESC
LIMIT 5;

-- I dont see big difference with analyze and without





--5. Query Optimization: Distribution Style & Sort Keys

--a) noding auto, sort by calendar_date 

-- create table with new parametrs
DROP TABLE IF EXISTS user_dilab_student56.report_trip_data;

CREATE TABLE user_dilab_student56.report_trip_data (
  calendar_date DATE,
  customer_type VARCHAR(50),
  trip_duration INT,
  trip_amount DECIMAL(10,2)
)
DISTSTYLE auto
SORTKEY (calendar_date); --redshift can sorting by date



-- turn off cash for quality test result (it is working directly from disk always)
SET enable_result_cache_for_session TO OFF;


--using analyze and see how time is changing
ANALYZE user_dilab_student56.fct_taxi_trips;
ANALYZE user_dilab_student56.dim_dates;
ANALYZE user_dilab_student56.dim_customers_scd;
ANALYZE user_dilab_student56.report_trip_data;


-- call procedure
CALL user_dilab_student56.generate_trip_report();

-- test time needed for query
SELECT starttime, endtime, DATEDIFF(ms, starttime, endtime) AS duration_ms
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%generate_trip_report%'
ORDER BY starttime DESC
LIMIT 5;

--check execution plan before optimization
explain
 SELECT 
    d.calendar_date,
    c.customer_type,
    SUM(t.trip_duration) AS total_duration,
    SUM(t.trip_amount) AS total_amount
  FROM user_dilab_student56.fct_taxi_trips t
  JOIN user_dilab_student56.dim_dates d 
    ON t.pickup_date_id = d.date_sur_id
  JOIN user_dilab_student56.dim_customers_scd c 
    ON t.customer_sur_id = c.customer_sur_id
  WHERE d.year = 2016
  GROUP BY d.calendar_date, c.customer_type
  ORDER BY d.calendar_date, c.customer_type;


--b) noding and sort by calendar_date 

-- create table with new parametrs
DROP TABLE IF EXISTS user_dilab_student56.report_trip_data;

CREATE TABLE user_dilab_student56.report_trip_data (
  calendar_date DATE,
  customer_type VARCHAR(50),
  trip_duration INT,
  trip_amount DECIMAL(10,2)
)
DISTSTYLE KEY
DISTKEY (calendar_date)
SORTKEY (calendar_date);


-- turn off cash for quality test result (it is working directly from disk always)
SET enable_result_cache_for_session TO OFF;


--using analyze and see how time is changing
ANALYZE user_dilab_student56.fct_taxi_trips;
ANALYZE user_dilab_student56.dim_dates;
ANALYZE user_dilab_student56.dim_customers_scd;
ANALYZE user_dilab_student56.report_trip_data;


-- call procedure
CALL user_dilab_student56.generate_trip_report();

-- test time needed for query
SELECT starttime, endtime, DATEDIFF(ms, starttime, endtime) AS duration_ms
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%generate_trip_report%'
ORDER BY starttime DESC
LIMIT 5;


-- *) optimize one table query (sorting compaund and interleaved)

-- test table for difference between compound and interleaved sorting
CREATE TABLE user_dilab_student56.fct_sort_test_compound (
    trip_sur_id BIGINT,
    pickup_date_id BIGINT,
    dropoff_date_id BIGINT,
    trip_duration INT
)
COMPOUND SORTKEY (pickup_date_id, dropoff_date_id);

CREATE TABLE user_dilab_student56.fct_sort_test_interleaved (
    trip_sur_id BIGINT,
    pickup_date_id BIGINT,
    dropoff_date_id BIGINT,
    trip_duration INT
)
INTERLEAVED SORTKEY (pickup_date_id, dropoff_date_id);

--cash is dont using for clear test results
SET enable_result_cache_for_session TO OFF;


--compound
INSERT INTO user_dilab_student56.fct_sort_test_compound
SELECT trip_sur_id, pickup_date_id, dropoff_date_id, trip_duration
FROM user_dilab_student56.fct_taxi_trips
LIMIT 100000;

EXPLAIN
SELECT *
FROM user_dilab_student56.fct_sort_test_compound
WHERE pickup_date_id BETWEEN 17500 AND 17600;

SELECT 
  query,
  starttime,
  endtime,
  DATEDIFF(ms, starttime, endtime) AS duration_ms,
  SUBSTRING(querytxt, 1, 100) AS preview
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%fct_sort_test_compound%'
ORDER BY starttime DESC
LIMIT 5;

--
VACUUM REINDEX user_dilab_student56.fct_sort_test_compound;
ANALYZE user_dilab_student56.fct_sort_test_compound;

explain
SELECT *
FROM user_dilab_student56.fct_sort_test_compound
WHERE pickup_date_id BETWEEN 17500 AND 17600;

SELECT 
  query,
  starttime,
  endtime,
  DATEDIFF(ms, starttime, endtime) AS duration_ms,
  SUBSTRING(querytxt, 1, 100) AS preview
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%fct_sort_test_compound%'
ORDER BY starttime DESC
LIMIT 5;


-- interleaved (always use analyze command after download big data for optimize)
INSERT INTO user_dilab_student56.fct_sort_test_interleaved
SELECT trip_sur_id, pickup_date_id, dropoff_date_id, trip_duration
FROM user_dilab_student56.fct_taxi_trips
LIMIT 100000;

EXPLAIN
SELECT *
FROM user_dilab_student56.fct_sort_test_interleaved
WHERE pickup_date_id BETWEEN 17500 AND 17600;

SELECT 
  query,
  starttime,
  endtime,
  DATEDIFF(ms, starttime, endtime) AS duration_ms,
  SUBSTRING(querytxt, 1, 100) AS preview
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%fct_sort_test_interleaved%'
ORDER BY starttime DESC
LIMIT 5;

--
VACUUM REINDEX user_dilab_student56.fct_sort_test_interleaved;
ANALYZE user_dilab_student56.fct_sort_test_interleaved;

explain
SELECT *
FROM user_dilab_student56.fct_sort_test_interleaved
WHERE pickup_date_id BETWEEN 17500 AND 17600;

SELECT 
  query,
  starttime,
  endtime,
  DATEDIFF(ms, starttime, endtime) AS duration_ms,
  SUBSTRING(querytxt, 1, 100) AS preview
FROM stl_query
WHERE userid > 1
  AND querytxt ILIKE '%fct_sort_test_interleaved%'
ORDER BY starttime DESC
LIMIT 5;




--COPY QUESTION – Redshift Performance Comparison

CREATE TABLE lineorder_1 (
  lo_orderkey INTEGER NOT NULL,
  lo_linenumber INTEGER NOT NULL,
  lo_custkey INTEGER NOT NULL,
  lo_partkey INTEGER NOT NULL,
  lo_suppkey INTEGER NOT NULL,
  lo_orderdate INTEGER NOT NULL,
  lo_orderpriority VARCHAR(15) NOT NULL,
  lo_shippriority VARCHAR(1) NOT NULL,
  lo_quantity INTEGER NOT NULL,
  lo_extendedprice INTEGER NOT NULL,
  lo_ordertotalprice INTEGER NOT NULL,
  lo_discount INTEGER NOT NULL,
  lo_revenue INTEGER NOT NULL,
  lo_supplycost INTEGER NOT NULL,
  lo_tax INTEGER NOT NULL,
  lo_commitdate INTEGER NOT NULL,
  lo_shipmode VARCHAR(10) NOT NULL
);

CREATE TABLE lineorder_2 ( 
  lo_orderkey INTEGER NOT NULL,
  lo_linenumber INTEGER NOT NULL,
  lo_custkey INTEGER NOT NULL,
  lo_partkey INTEGER NOT NULL,
  lo_suppkey INTEGER NOT NULL,
  lo_orderdate INTEGER NOT NULL,
  lo_orderpriority VARCHAR(15) NOT NULL,
  lo_shippriority VARCHAR(1) NOT NULL,
  lo_quantity INTEGER NOT NULL,
  lo_extendedprice INTEGER NOT NULL,
  lo_ordertotalprice INTEGER NOT NULL,
  lo_discount INTEGER NOT NULL,
  lo_revenue INTEGER NOT NULL,
  lo_supplycost INTEGER NOT NULL,
  lo_tax INTEGER NOT NULL,
  lo_commitdate INTEGER NOT NULL,
  lo_shipmode VARCHAR(10) NOT NULL
);


-- 1. COPY in lineorder_1
COPY lineorder_1
FROM 's3://dilabbucket/files/lineorder_file/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER '|'
IGNOREHEADER 1;

-- 2. COPY in lineorder_2
COPY lineorder_2
FROM 's3://dilabbucket/files/lineorder/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER '|'
IGNOREHEADER 1;


SELECT query, starttime, endtime, DATEDIFF(ms, starttime, endtime) AS duration_ms
FROM stl_query
WHERE querytxt ILIKE '%COPY lineorder%'
  AND userid > 1
ORDER BY starttime DESC
LIMIT 5;

select * from stl_query;




-----------------------------------------------
--WORKING WITH EXTERNAL TABLES

-- created external schema(it like bridge fromRedshift and S3 via Glue ... we can read and dont adding to Redshift)
CREATE EXTERNAL SCHEMA IF NOT EXISTS user_dilab_student56_ext
FROM DATA CATALOG
DATABASE 'bl_dm_oleg_arslanov'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role';

-- created external schema (without adding files from S3)
--CREATE EXTERNAL SCHEMA IF NOT EXISTS user_dilab_student56_ext
--FROM DATA CATALOG
--DATABASE 'bl_dm_oleg_arslanov'
--IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-glue-role';


--create partition data by month and unload to S3 (for later use for partitioned table)
UNLOAD ('
  SELECT *
  FROM user_dilab_student56.fct_taxi_trips t
  join user_dilab_student56.dim_dates d 
	on t.pickup_date_id = d.date_sur_id
  WHERE d.year = 2016 and d.month = 1
')
TO 's3://student-oleg-arslanov-bucket/spectrum_data_partitioned/dt=2016-01-01/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
DELIMITER ',' 
ALLOWOVERWRITE 
PARALLEL OFF 
HEADER;

UNLOAD ('
  SELECT *
  FROM user_dilab_student56.fct_taxi_trips t
  join user_dilab_student56.dim_dates d 
	on t.pickup_date_id = d.date_sur_id
  WHERE d.year = 2016 and d.month = 2
')
TO 's3://student-oleg-arslanov-bucket/spectrum_data_partitioned/dt=2016-02-01/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
DELIMITER ',' 
ALLOWOVERWRITE 
PARALLEL OFF 
HEADER;

--create external table with partition, that stored in user_dilab_student56_ext schema (connected to S3 data)
CREATE EXTERNAL TABLE user_dilab_student56_ext.ext_partitioned_trips (
    trip_sur_id BIGINT,
    vendor_sur_id BIGINT,
    customer_sur_id BIGINT,
    junk_sur_id BIGINT,
    rate_sur_id BIGINT,
    promo_sur_id BIGINT,
    pickup_location_id BIGINT,
    dropoff_location_id BIGINT,
    booking_date_id BIGINT,
    pickup_date_id BIGINT,
    dropoff_date_id BIGINT,
    payment_date_id BIGINT,
    pickup_time_id BIGINT,
    dropoff_time_id BIGINT,
    booking_time_id BIGINT,
    payment_time_id BIGINT,
    trip_src_id BIGINT,
    trip_id VARCHAR(50),
    trip_duration INT,
    passenger_count INT,
    distance_miles DECIMAL(10,2),
    trip_amount DECIMAL(10,2),
    update_dt TIMESTAMP,
    insert_dt TIMESTAMP
)
PARTITIONED BY (dt varchar)  -- here our folder dt=2016-01-01 in S3 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://student-oleg-arslanov-bucket/spectrum_data_partitioned/'
TABLE PROPERTIES ('skip.header.line.count'='1');

--added partitions
ALTER TABLE user_dilab_student56_ext.ext_partitioned_trips
ADD PARTITION (dt = '2016-01-01')
LOCATION 's3://student-oleg-arslanov-bucket/spectrum_data_partitioned/dt=2016-01-01/';

ALTER TABLE user_dilab_student56_ext.ext_partitioned_trips
ADD PARTITION (dt = '2016-02-01')
LOCATION 's3://student-oleg-arslanov-bucket/spectrum_data_partitioned/dt=2016-02-01/';

-- check partitions all working or not
SELECT COUNT(*) FROM user_dilab_student56_ext.ext_partitioned_trips WHERE dt = '2016-01-01';
SELECT COUNT(*) FROM user_dilab_student56_ext.ext_partitioned_trips WHERE dt = '2016-02-01';


--explain query
EXPLAIN
SELECT *
FROM user_dilab_student56_ext.ext_partitioned_trips
WHERE dt = '2016-01-01';


EXPLAIN
SELECT *
FROM user_dilab_student56_ext.ext_partitioned_trips;
