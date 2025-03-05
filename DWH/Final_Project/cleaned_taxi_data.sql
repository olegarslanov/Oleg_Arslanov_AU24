begin;
--rollback;
--create clened table from combined_taxi_data


CREATE TABLE IF NOT EXISTS bl_cl.cleaned_taxi_data AS
SELECT 
    trip_src_id, 
    vendor_src_id, 
    vendor_name, 
    vendor_address_src_id, 
    street as vendor_street, 
    house AS vendor_house, 
    city AS vendor_city, 
    country AS vendor_country, 
    postal_code AS vendor_postal_code,
    vendor_telephone,
    -- modify to timestamp в TIMESTAMP (NULL is NULL)
    CASE 
        WHEN pickup_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$'
        THEN TO_TIMESTAMP(pickup_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone  
        ELSE NULL 
    END AS pickup_datetime,
    CASE 
        WHEN dropoff_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(dropoff_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone 
        ELSE NULL 
    END AS dropoff_datetime,
    passenger_count::INTEGER,
    location_src_id, 
    pickup_longitude::DECIMAL(10,2), 
    pickup_latitude::DECIMAL(10,2), 
    dropoff_longitude::DECIMAL(10,2), 
    dropoff_latitude::DECIMAL(10,2), 
    distance_miles::DECIMAL(10,2), 
    trip_duration::INTEGER, 
	COALESCE(NULLIF(booking_src_id, 'NULL'), 'n.a.') AS booking_src_id,
	COALESCE(booking_type, 'n.a.') AS booking_type,
    -- modify rows in TIMESTAMP
    CASE 
        WHEN booking_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(booking_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone  
        ELSE NULL 
    END AS booking_datetime,
    rate_src_id,
    base_fare::DECIMAL(10,2), 
    rate_per_mile::DECIMAL(10,2), 
    payment_src_id, 
    payment_type,
    -- modify rows in TIMESTAMP
    CASE 
        WHEN payment_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(payment_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone  
        ELSE NULL 
    END AS payment_datetime,
    COALESCE(NULLIF(customer_src_id, 'NULL'), 'n.a.') AS customer_src_id,
	COALESCE(customer_type, 'n.a.') AS customer_type,
	COALESCE(NULLIF(customer_telephone, ''), 'n.a.') AS customer_telephone,
    COALESCE(NULLIF(promo_src_id, 'NULL'), 'n.a.') AS promo_src_id,
    COALESCE(NULLIF(promo_code, 'NULL'), 'n.a.') AS promo_code,
    -- modify discount_percentage in INTEGER and check that it is in interval (0-100)
    CASE 
	    WHEN discount_percentage ~ '^\d+$' AND discount_percentage::INTEGER BETWEEN 0 AND 100 
	    THEN discount_percentage::INTEGER 
	    ELSE 0 
	END AS discount_percentage, 
    -- add source_system and source_entity
	source_system,
	source_entity
FROM bl_cl.combined_taxi_data;

--need to create index because i want use 'on conflict' so column must be unique
CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_src_id
ON bl_cl.cleaned_taxi_data (trip_src_id);




--insert from combined_taxi_data into cleaned_taxi_data
INSERT INTO bl_cl.cleaned_taxi_data 
SELECT *
from (
    SELECT 
    trip_src_id, 
    vendor_src_id, 
    vendor_name, 
    vendor_address_src_id, 
    street as vendor_street, 
    house AS vendor_house, 
    city AS vendor_city, 
    country AS vendor_country, 
    postal_code AS vendor_postal_code,
    vendor_telephone,
    -- modify to timestamp в TIMESTAMP (NULL is NULL)
    CASE 
        WHEN pickup_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(pickup_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone  
        ELSE NULL 
    END AS pickup_datetime,
    CASE 
        WHEN dropoff_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(dropoff_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone  
        ELSE NULL 
    END AS dropoff_datetime,
    passenger_count::INTEGER,
    location_src_id, 
    pickup_longitude::DECIMAL(10,2), 
    pickup_latitude::DECIMAL(10,2), 
    dropoff_longitude::DECIMAL(10,2), 
    dropoff_latitude::DECIMAL(10,2), 
    distance_miles::DECIMAL(10,2), 
    trip_duration::INTEGER, 
	COALESCE(NULLIF(booking_src_id, 'NULL'), 'n.a.') AS booking_src_id,
	COALESCE(booking_type, 'n.a.') AS booking_type,
    -- modify rows in TIMESTAMP
    CASE 
        WHEN booking_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(booking_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone 
        ELSE NULL 
    END AS booking_datetime,
    rate_src_id,
    base_fare::DECIMAL(10,2), 
    rate_per_mile::DECIMAL(10,2), 
    payment_src_id, 
    payment_type,
    -- modify rows in TIMESTAMP
    CASE 
        WHEN payment_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(payment_datetime, 'DD/MM/YYYY HH24:MI'):: timestamp without time zone 
        ELSE NULL 
    END AS payment_datetime,
    COALESCE(NULLIF(customer_src_id, 'NULL'), 'n.a.') AS customer_src_id,
	COALESCE(customer_type, 'n.a.') AS customer_type,
	COALESCE(NULLIF(ctd.customer_telephone, ''), 'n.a.') AS customer_telephone,
    COALESCE(NULLIF(promo_src_id, 'NULL'), 'n.a.') AS promo_src_id,
    COALESCE(NULLIF(promo_code, 'NULL'), 'n.a.') AS promo_code,
    -- modify discount_percentage in INTEGER and check that it is in interval (0-100)
    CASE 
	    WHEN discount_percentage ~ '^\d+$' AND discount_percentage::INTEGER BETWEEN 0 AND 100 
	    THEN discount_percentage::INTEGER 
	    ELSE 0 
	END AS discount_percentage,
    -- add source_system and source_entity
	source_system,
	source_entity
FROM bl_cl.combined_taxi_data ctd
--ORDER BY random()  -- Randomize rows
    --LIMIT 100000         -- Select only 1000
) AS new_data
ON CONFLICT (trip_src_id)
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


-- in study case I delete this problematic date row (because too much time on it)
DELETE FROM bl_cl.cleaned_taxi_data
WHERE trip_src_id = 'id0003576';




commit;




-------------------------------------------------------------------------------------
-- Here my drafts

-- For insert I used this add for load without duplicates
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_cl.cleaned_taxi_data ctd
    WHERE ctd.trip_src_id = new_data.trip_src_id
);



--select and others
select * from bl_cl.cleaned_taxi_data; 
SELECT count(*) FROM bl_cl.cleaned_taxi_data;
--DROP TABLE IF EXISTS bl_cl.cleaned_taxi_data CASCADE;
truncate table bl_cl.cleaned_taxi_data;

--look about query what is going on
SELECT pid, now() - query_start AS duration, state, wait_event, query
FROM pg_stat_activity
WHERE state != 'idle' AND query ILIKE '%INSERT INTO bl_3nf.ce_vendors%';

DELETE FROM bl_cl.cleaned_taxi_data WHERE trip_src_id IS NULL;



select min(pickup_datetime) from bl_cl.cleaned_taxi_data;





-----------------------------------------------------------------------------------------
--Create cleaned_taxi_data partitions by half month

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


CREATE TABLE bl_cl.cleaned_taxi_data_2016_01_01_05 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2016-01-05 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_01_end PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-01-16 00:00:00') TO ('2016-01-31 23:59:59');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_02_start PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-02-01') TO ('2016-02-15');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_02_end PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-02-15') TO ('2016-03-01');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_03 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-03-01') TO ('2016-03-15');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_03 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-03-01') TO ('2016-04-01');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_04 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-04-01') TO ('2016-05-01');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_05 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-05-01') TO ('2016-06-01');

CREATE TABLE bl_cl.cleaned_taxi_data_2016_06 PARTITION OF bl_cl.cleaned_taxi_data_partition 
    FOR VALUES FROM ('2016-06-01') TO ('2016-07-01');


drop table bl_cl.cleaned_taxi_data_2016_01_start;

--create unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_cleaned_taxi_data_trip_src_id_pickup_datetime
ON bl_cl.cleaned_taxi_data_partition (trip_src_id, pickup_datetime);


--4. atach complex primary key to parental table
ALTER table bl_cl.cleaned_taxi_data_partition
    ADD CONSTRAINT pk_cleaned_taxi_data_partition 
    PRIMARY KEY (trip_src_id, pickup_datetime);


-- insert data by month
INSERT INTO bl_cl.cleaned_taxi_data_partition 
SELECT *
from bl_cl.cleaned_taxi_data ctd
where ctd.pickup_datetime BETWEEN '2016-01-01 00:00:00' AND '2016-01-05 23:59:59'
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


select count(*) from bl_cl.cleaned_taxi_data_partition;
select * from bl_cl.cleaned_taxi_data_partition;

select count(*) from bl_cl.cleaned_taxi_data_2016_01_start;
select count(*) from bl_cl.cleaned_taxi_data_2016_01_end;


SELECT
    inhrelid::regclass AS partition_name,
    parent.relname AS parent_table,
    child.relname AS child_table
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'cleaned_taxi_data_partition';











