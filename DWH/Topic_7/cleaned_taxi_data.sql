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
        THEN TO_TIMESTAMP(pickup_datetime, 'DD/MM/YYYY HH24:MI') 
        ELSE NULL 
    END AS pickup_datetime,
    CASE 
        WHEN dropoff_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(dropoff_datetime, 'DD/MM/YYYY HH24:MI') 
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
        THEN TO_TIMESTAMP(booking_datetime, 'DD/MM/YYYY HH24:MI') 
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
        THEN TO_TIMESTAMP(payment_datetime, 'DD/MM/YYYY HH24:MI') 
        ELSE NULL 
    END AS payment_datetime,
    COALESCE(NULLIF(customer_src_id, 'NULL'), 'n.a.') AS customer_src_id,
	COALESCE(customer_type, 'n.a.') AS customer_type,
	COALESCE(customer_telephone, 'n.a.') AS customer_telephone,
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



--insert from combined_taxi_data
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
        THEN TO_TIMESTAMP(pickup_datetime, 'DD/MM/YYYY HH24:MI') 
        ELSE NULL 
    END AS pickup_datetime,
    CASE 
        WHEN dropoff_datetime ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$' 
        THEN TO_TIMESTAMP(dropoff_datetime, 'DD/MM/YYYY HH24:MI') 
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
        THEN TO_TIMESTAMP(booking_datetime, 'DD/MM/YYYY HH24:MI') 
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
        THEN TO_TIMESTAMP(payment_datetime, 'DD/MM/YYYY HH24:MI') 
        ELSE NULL 
    END AS payment_datetime,
    COALESCE(NULLIF(customer_src_id, 'NULL'), 'n.a.') AS customer_src_id,
	COALESCE(customer_type, 'n.a.') AS customer_type,
	COALESCE(customer_telephone, 'n.a.') AS customer_telephone,
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
FROM bl_cl.combined_taxi_data
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

commit;




-------------------------------------------------------------------------------------
-- Here my drafts

-- For insert I used this add for load without duplicates
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_cl.cleaned_taxi_data ctd
    WHERE ctd.trip_src_id = new_data.trip_src_id
);

-- For insert when data is changed I use this add
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
    discount_percentage = EXCLUDED.discount_percentage,
    source_system = EXCLUDED.source_system,
    source_entity = EXCLUDED.source_entity;


--select and others
select * from bl_cl.cleaned_taxi_data; 
DROP TABLE IF EXISTS bl_cl.cleaned_taxi_data CASCADE;
truncate table bl_cl.cleaned_taxi_data;

--look about query what is going on
SELECT pid, now() - query_start AS duration, state, wait_event, query
FROM pg_stat_activity
WHERE state != 'idle' AND query ILIKE '%INSERT INTO bl_3nf.ce_vendors%';

DELETE FROM bl_cl.cleaned_taxi_data
WHERE trip_src_id IS NULL;

SELECT * 
FROM bl_cl.cleaned_taxi_data
WHERE vendor_address_src_id IS NULL;