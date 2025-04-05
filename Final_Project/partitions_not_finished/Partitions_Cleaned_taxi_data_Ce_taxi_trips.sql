------------------------------------------------------------------------------------------------------------
--Create cleaned_taxi_data partitions by month

CREATE TABLE bl_cl.cleaned_taxi_data_partition (
    trip_src_id VARCHAR,
    vendor_src_id VARCHAR,
    vendor_name VARCHAR,
    vendor_address_src_id VARCHAR,
    vendor_street VARCHAR,
    vendor_house VARCHAR,
    vendor_city VARCHAR,
    vendor_country VARCHAR,
    vendor_postal_code VARCHAR,
    vendor_telephone VARCHAR,
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    passenger_count INTEGER,
    location_src_id VARCHAR,
    pickup_longitude DECIMAL(10,2),
    pickup_latitude DECIMAL(10,2),
    dropoff_longitude DECIMAL(10,2),
    dropoff_latitude DECIMAL(10,2),
    distance_miles DECIMAL(10,2),
    trip_duration INTEGER,
    booking_src_id VARCHAR,
    booking_type VARCHAR,
    booking_datetime TIMESTAMP WITHOUT TIME ZONE,
    rate_src_id VARCHAR,
    base_fare DECIMAL(10,2),
    rate_per_mile DECIMAL(10,2),
    payment_src_id VARCHAR,
    payment_type VARCHAR,
    payment_datetime TIMESTAMP WITHOUT TIME ZONE,
    customer_src_id VARCHAR,
    customer_type VARCHAR,
    customer_telephone VARCHAR,
    promo_src_id VARCHAR,
    promo_code VARCHAR,
    discount_percentage INTEGER,
    source_system VARCHAR,
    source_entity VARCHAR
) PARTITION BY RANGE (pickup_datetime);

--DROP TABLE IF EXISTS bl_cl.cleaned_taxi_data_partition CASCADE;


CREATE TABLE bl_cl.cleaned_taxi_data_2016_01 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2016-01-31 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_02 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-02-01 00:00:00') TO ('2016-02-29 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_03 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-03-01 00:00:00') TO ('2016-03-31 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_04 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-04-01 00:00:00') TO ('2016-04-30 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_05 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-05-01 00:00:00') TO ('2016-05-31 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_06 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-06-01 00:00:00') TO ('2016-06-30 23:59:59');


--drop table bl_cl.cleaned_taxi_data_2016_02_start;

--create unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_cleaned_taxi_data_trip_src_id_pickup_datetime
ON bl_cl.cleaned_taxi_data_partition (trip_src_id, pickup_datetime);


--4. atach complex primary key to parental table
--ALTER table bl_cl.cleaned_taxi_data_partition
    --ADD CONSTRAINT pk_cleaned_taxi_data_partition 
    --PRIMARY KEY (trip_src_id, pickup_datetime);


-- insert data by month
INSERT INTO bl_cl.cleaned_taxi_data_partition 
SELECT *
from bl_cl.cleaned_taxi_data ctd
where ctd.pickup_datetime BETWEEN '2016-06-01 00:00:00' AND '2016-06-30 23:59:59'
ON CONFLICT (trip_src_id, pickup_datetime)
DO UPDATE SET
    vendor_src_id = EXCLUDED.vendor_src_id,
    vendor_name = EXCLUDED.vendor_name,
    vendor_address_src_id = EXCLUDED.vendor_address_src_id,
    vendor_street = EXCLUDED.vendor_street,
    vendor_house = EXCLUDED.vendor_house,
    vendor_city = EXCLUDED.vendor_city,
    vendor_country = EXCLUDED.vendor_country,
    vendor_postal_code = EXCLUDED.vendor_postal_code,
    vendor_telephone = EXCLUDED.vendor_telephone,
    pickup_datetime = EXCLUDED.pickup_datetime,
    dropoff_datetime = EXCLUDED.dropoff_datetime,
    passenger_count = EXCLUDED.passenger_count,
    location_src_id = EXCLUDED.location_src_id,
    pickup_longitude = EXCLUDED.pickup_longitude,
    pickup_latitude = EXCLUDED.pickup_latitude,
    dropoff_longitude = EXCLUDED.dropoff_longitude,
    dropoff_latitude = EXCLUDED.dropoff_latitude,
    distance_miles = EXCLUDED.distance_miles,
    trip_duration = EXCLUDED.trip_duration,
    booking_src_id = EXCLUDED.booking_src_id,
    booking_type = EXCLUDED.booking_type,
    booking_datetime = EXCLUDED.booking_datetime,
    rate_src_id = EXCLUDED.rate_src_id,
    base_fare = EXCLUDED.base_fare,
    rate_per_mile = EXCLUDED.rate_per_mile,
    payment_src_id = EXCLUDED.payment_src_id,
    payment_type = EXCLUDED.payment_type,
    payment_datetime = EXCLUDED.payment_datetime,
    customer_src_id = EXCLUDED.customer_src_id,
    customer_type = EXCLUDED.customer_type,
    customer_telephone = EXCLUDED.customer_telephone,
    promo_src_id = EXCLUDED.promo_src_id,
    promo_code = EXCLUDED.promo_code,
    discount_percentage = EXCLUDED.discount_percentage;


--check how much rows in partition
select count(*) from bl_cl.cleaned_taxi_data_partition;
select count(*) from bl_cl.cleaned_taxi_data_2016_01;
select count(*) from bl_cl.cleaned_taxi_data_2016_02;
select count(*) from bl_cl.cleaned_taxi_data_2016_03;
select count(*) from bl_cl.cleaned_taxi_data_2016_04;
select count(*) from bl_cl.cleaned_taxi_data_2016_05;
select count(*) from bl_cl.cleaned_taxi_data_2016_06;

select * from bl_cl.cleaned_taxi_data_partition;


















-- check parent/child tables
SELECT
    inhrelid::regclass AS partition_name,
    parent.relname AS parent_table,
    child.relname AS child_table
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'cleaned_taxi_data_partition';

truncate table bl_cl.cleaned_taxi_data_2016_01;
truncate table bl_cl.cleaned_taxi_data_2016_02;
truncate table bl_cl.cleaned_taxi_data_2016_03;
truncate table bl_cl.cleaned_taxi_data_2016_04;
truncate table bl_cl.cleaned_taxi_data_2016_05;
truncate table bl_cl.cleaned_taxi_data_2016_06;

----------------------------------------------------------------------------------------------------------------
----Create ce_taxi_trips partitions by month

-- 1. create parental table with partition (this table dont hold data, this table is only logical point for child table data ... only children tables hold data)
CREATE TABLE IF NOT EXISTS bl_3nf.ce_taxi_trips_partition (
    trip_id               BIGINT         NOT NULL,   -- key from 3NF
    vendor_id             BIGINT         NOT NULL,
    booking_id            BIGINT         NULL,
    customer_id           BIGINT         NOT NULL,
    promo_id              BIGINT         NULL,
    payment_id            BIGINT         NOT NULL,
    rate_id               BIGINT         NOT NULL,
    pickup_location_id    BIGINT         NOT NULL,
    dropoff_location_id   BIGINT         NOT NULL,
    pickup_datetime       TIMESTAMP      NOT NULL,   -- column for partition
    dropoff_datetime      TIMESTAMP      NULL,
    distance_miles        DECIMAL(10,2)  NOT NULL,
    trip_duration         INT            NOT NULL,
    passenger_count       INT            NOT NULL,
    trip_src_id           VARCHAR(255)   NOT NULL,
    source_system         VARCHAR(255)   NOT NULL,
    source_entity         VARCHAR(255)   NOT NULL,
    customer_start_dt     TIMESTAMP      NOT NULL,
    update_dt             TIMESTAMP      NOT NULL,
    insert_dt             TIMESTAMP      NOT NULL,
    CONSTRAINT chk_ce_taxi_trips_partition_time CHECK (dropoff_datetime > pickup_datetime),
    CONSTRAINT ce_taxi_trips_partition_distance_miles_check CHECK (distance_miles >= 0)
) PARTITION BY RANGE (pickup_datetime);

--drop table bl_3nf.ce_taxi_trips_partition cascade;
--truncate table bl_3nf.ce_taxi_trips_partition;

CREATE TABLE bl_3nf.ce_taxi_trips_2016_01 PARTITION OF bl_3nf.ce_taxi_trips_partition 
    FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2016-01-31 23:59:59');

CREATE TABLE bl_3nf.ce_taxi_trips_2016_02 PARTITION OF bl_3nf.ce_taxi_trips_partition
    FOR VALUES FROM ('2016-02-01 00:00:00') TO ('2016-02-29 23:59:59');

CREATE TABLE bl_3nf.ce_taxi_trips_2016_03 PARTITION OF bl_3nf.ce_taxi_trips_partition 
    FOR VALUES FROM ('2016-03-01 00:00:00') TO ('2016-03-31 23:59:59');

CREATE TABLE bl_3nf.ce_taxi_trips_2016_04 PARTITION OF bl_3nf.ce_taxi_trips_partition 
    FOR VALUES FROM ('2016-04-01 00:00:00') TO ('2016-04-30 23:59:59');

CREATE TABLE bl_3nf.ce_taxi_trips_2016_05 PARTITION OF bl_3nf.ce_taxi_trips_partition 
    FOR VALUES FROM ('2016-05-01 00:00:00') TO ('2016-05-31 23:59:59');

CREATE TABLE bl_3nf.ce_taxi_trips_2016_06 PARTITION OF bl_3nf.ce_taxi_trips_partition 
    FOR VALUES FROM ('2016-06-01 00:00:00') TO ('2016-06-30 23:59:59');



--create unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_taxi_trips_datetime_trip_src_id
ON bl_dm.fct_taxi_trips_partition (trip_src_id, pickup_datetime);








--check how much rows in partition
select count(*) from bl_3nf.ce_taxi_trips_partition;
select count(*) from bl_3nf.ce_taxi_trips_2016_01;
select count(*) from bl_3nf.ce_taxi_trips_2016_02;
select count(*) from bl_3nf.ce_taxi_trips_2016_03;
select count(*) from bl_3nf.ce_taxi_trips_2016_04;
select count(*) from bl_3nf.ce_taxi_trips_2016_05;
select count(*) from bl_3nf.ce_taxi_trips_2016_06;

select * from bl_cl.cleaned_taxi_data_partition;


truncate table bl_3nf.ce_taxi_trips_2016_01;

-- check parent/child tables
SELECT
    inhrelid::regclass AS partition_name,
    parent.relname AS parent_table,
    child.relname AS child_table
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'ce_taxi_trips_partition';


SELECT attname, avg_width 
FROM pg_stats 
WHERE tablename = 'ce_taxi_trips_2016_01' 
ORDER BY avg_width DESC;

SELECT relname AS index_name, pg_size_pretty(pg_relation_size(oid)) 
FROM pg_class 
WHERE relkind = 'i' AND relname LIKE 'idx_fct_taxi_trips_datetime_trip_src_id';

SELECT 'ce_taxi_trips_2016_01', count(*) 
FROM pg_inherits 
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid 
JOIN pg_class child ON pg_inherits.inhrelid = child.oid 
WHERE parent.relname = 'ce_taxi_trips_partition'; 
GROUP BY 'ce_taxi_trips_2016_01';

vacuum full;
