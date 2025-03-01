--create green taxi schema
create schema if not exists sa_green_taxi;

-- create add for working with external files
create extension if not exists file_fdw;

--create server for reference from file
do $$
begin
	if not exists (select 1 from pg_foreign_server WHERE srvname = 'green_trip_file_server') 
	then CREATE SERVER green_trip_file_server FOREIGN DATA WRAPPER file_fdw;
    END IF;
end $$;

-- create external table (this table dont hold data, only reference from external source)
create foreign table if not exists sa_green_taxi.ext_green_trip (
    trip_src_id varchar,
    vendor_src_id varchar,
    vendor varchar,
    vendor_address_src_id varchar,
    street varchar,
    house varchar,
    city varchar,
    country varchar,
    postal_code varchar,
    vendor_telephone varchar,
    pickup_datetime varchar,
    dropoff_datetime varchar,
    passengers varchar,
    location_src_id varchar,
    pickup_longitude varchar,
    pickup_latitude varchar,
    dropoff_longitude varchar,
    dropoff_latitude varchar,
    distance_miles varchar,
    trip_duration varchar,
    booking_src_id varchar,
    booking_type varchar,
    booking_datetime varchar,
    rate_src_id varchar,
    base_fare varchar,
    rate_per_mile varchar,
    payment_src_id varchar,
    payment_type varchar,
    customer_src_id varchar,
    customer_type varchar,
    customer_telephone varchar,
    promo_src_id varchar,
    promo_code varchar,
    discount_percentage varchar
)
server green_trip_file_server
options (filename 'C:\Program Files\PostgreSQL\17\data\EPAM_study\green_taxi_data.csv', format 'csv', header 'true');

--create table inside Postgre with raw data 
create table if not exists sa_green_taxi.src_green_trip (
    trip_src_id varchar,
    vendor_src_id varchar,
    vendor varchar,
    vendor_address_src_id varchar,
    street varchar,
    house varchar,
    city varchar,
    country varchar,
    postal_code varchar,
    vendor_telephone varchar,
    pickup_datetime varchar,
    dropoff_datetime varchar,
    passengers varchar,
    location_src_id varchar,
    pickup_longitude varchar,
    pickup_latitude varchar,
    dropoff_longitude varchar,
    dropoff_latitude varchar,
    distance_miles varchar,
    trip_duration varchar,
    booking_src_id varchar,
    booking_type varchar,
    booking_datetime varchar,
    rate_src_id varchar,
    base_fare varchar,
    rate_per_mile varchar,
    payment_src_id varchar,
    payment_type varchar,
    customer_src_id varchar,
    customer_type varchar,
    customer_telephone varchar,
    promo_src_id varchar,
    promo_code varchar,
    discount_percentage varchar
);


--download data from external source (firstly we extract data to extension and then we can got it in our db, without duples)
insert into sa_green_taxi.src_green_trip
select * 
from sa_green_taxi.ext_green_trip t
where not exists (
    select 1
    from sa_green_taxi.src_green_trip s
    where s.trip_src_id = t.trip_src_id
);

-- copy I dont use because it is making duplication ( I can use but it is complicated ... need to made temp table, then copy, then delete duples)

--copy sa_green_taxi.src_green_trip
--from 'C:\Program Files\PostgreSQL\17\data\EPAM_study\green_taxi_data.csv'
--with (
    --format csv,
    --header true,
    --delimiter ','
--);

-- added source
ALTER TABLE sa_green_taxi.src_green_trip ADD COLUMN source_system VARCHAR(50) DEFAULT 'sa_green_taxi';
ALTER TABLE sa_green_taxi.src_green_trip ADD COLUMN source_entity VARCHAR(50) DEFAULT 'src_green_trip';


--one null row occur in this table , so I delete it for no errors
delete from sa_green_taxi.src_green_trip where trip_src_id is null;


----------------------------------------------------------------

--create yellow taxi schema
create schema if not exists sa_yellow_taxi;

-- create add for working with external files
--create extension if not exists file_fdw;

--create server for reference from file
do $$
begin
	if not exists (select 1 from pg_foreign_server WHERE srvname = 'yellow_trip_file_server') 
	then CREATE SERVER yellow_trip_file_server FOREIGN DATA WRAPPER file_fdw;
    END IF;
end $$;

-- create external table (this table dont hold data, only reference from external source)

create foreign table if not exists sa_yellow_taxi.ext_yellow_trip (
    trip_src_id varchar,
    vendor_src_id varchar,
    vendor_name varchar,
    vendor_address_src_id varchar,
    street varchar,
    house varchar,
    city varchar,
    country varchar,
    postal_code varchar,
    vendor_telephone varchar,
    pickup_datetime varchar,
    dropoff_datetime varchar,
    passenger_count varchar,
    location_src_id varchar,
    pickup_longitude varchar,
    pickup_latitude varchar,
    dropoff_longitude varchar,
    dropoff_latitude varchar,
    distance_miles varchar,
    trip_duration varchar,
    rate_src_id varchar,
    base_fare varchar,
    rate_per_mile varchar,
    payment_src_id varchar,
    payment_type varchar,
    payment_datetime varchar
)
server yellow_trip_file_server
options (filename 'C:\Program Files\PostgreSQL\17\data\EPAM_study\yellow_taxi_data.csv', format 'csv', header 'true');


---------------------------------------------------------------------------------------------------------------------------
--create table inside Postgre with raw data 
create table if not exists sa_yellow_taxi.src_yellow_trip (
    trip_src_id varchar,
    vendor_src_id varchar,
    vendor_name varchar,
    vendor_address_src_id varchar,
    street varchar,
    house varchar,
    city varchar,
    country varchar,
    postal_code varchar,
    vendor_telephone varchar,
    pickup_datetime varchar,
    dropoff_datetime varchar,
    passenger_count varchar,
    location_src_id varchar,
    pickup_longitude varchar,
    pickup_latitude varchar,
    dropoff_longitude varchar,
    dropoff_latitude varchar,
    distance_miles varchar,
    trip_duration varchar,
    rate_src_id varchar,
    base_fare varchar,
    rate_per_mile varchar,
    payment_src_id varchar,
    payment_type varchar,
    payment_datetime varchar
);


--download data from external source 
insert into sa_yellow_taxi.src_yellow_trip
select * 
from sa_yellow_taxi.ext_yellow_trip t
where not exists (
    select 1
    from sa_yellow_taxi.src_yellow_trip s
    where s.trip_src_id = t.trip_src_id
);

--added source
ALTER TABLE sa_yellow_taxi.src_yellow_trip ADD COLUMN source_system VARCHAR(50) DEFAULT 'sa_yellow_taxi';
ALTER TABLE sa_yellow_taxi.src_yellow_trip ADD COLUMN source_entity VARCHAR(50) DEFAULT 'src_yellow_trip';


---------------------------------------------------------------------------------
--create cleansing level schema
create schema if not exists BL_CL;

--here I making simple deduplication by trip_src_id and combine data from two sources

CREATE TABLE IF NOT EXISTS bl_cl.combined_taxi_data AS
SELECT trip_src_id, MIN(vendor_src_id) AS vendor_src_id, MIN(vendor_name) AS vendor_name, MIN(vendor_address_src_id) AS vendor_address_src_id,
       MIN(street) AS street, MIN(house) AS house, MIN(city) AS city, MIN(country) AS country, MIN(postal_code) AS postal_code,
       MIN(vendor_telephone) AS vendor_telephone, MIN(pickup_datetime) AS pickup_datetime, MIN(dropoff_datetime) AS dropoff_datetime,
       MIN(passenger_count) AS passenger_count, MIN(location_src_id) AS location_src_id, MIN(pickup_longitude) AS pickup_longitude,
       MIN(pickup_latitude) AS pickup_latitude, MIN(dropoff_longitude) AS dropoff_longitude, MIN(dropoff_latitude) AS dropoff_latitude,
       MIN(distance_miles) AS distance_miles, MIN(trip_duration) AS trip_duration, MIN(booking_src_id) AS booking_src_id,
       MIN(booking_type) AS booking_type, MIN(booking_datetime) AS booking_datetime, MIN(rate_src_id) AS rate_src_id, 
       MIN(base_fare) AS base_fare, MIN(rate_per_mile) AS rate_per_mile, MIN(payment_src_id) AS payment_src_id, MIN(payment_type) AS payment_type,
       MIN(payment_datetime) AS payment_datetime, MIN(customer_src_id) AS customer_src_id, MIN(customer_type) AS customer_type, 
       MIN(customer_telephone) AS customer_telephone, MIN(promo_src_id) AS promo_src_id, MIN(promo_code) AS promo_code,
       MIN(discount_percentage) AS discount_percentage, min(source_system) as source_system, min(source_entity) as source_entity
FROM (
    SELECT trip_src_id, vendor_src_id, vendor AS vendor_name, vendor_address_src_id, street, house, city, country, postal_code,
           vendor_telephone, pickup_datetime, dropoff_datetime, passengers AS passenger_count, location_src_id, pickup_longitude,
           pickup_latitude, dropoff_longitude, dropoff_latitude, distance_miles, trip_duration, booking_src_id, booking_type,
           booking_datetime, rate_src_id, base_fare, rate_per_mile, payment_src_id, payment_type, NULL AS payment_datetime,
           customer_src_id, customer_type, customer_telephone, promo_src_id, promo_code, discount_percentage, source_system, source_entity
    FROM sa_green_taxi.src_green_trip
    UNION ALL
    SELECT trip_src_id, vendor_src_id, vendor_name AS vendor_name, vendor_address_src_id, street, house, city, country, postal_code,
           vendor_telephone, pickup_datetime, dropoff_datetime, passenger_count, location_src_id, pickup_longitude, pickup_latitude,
           dropoff_longitude, dropoff_latitude, distance_miles, trip_duration, NULL AS booking_src_id, NULL AS booking_type,
           NULL AS booking_datetime, rate_src_id, base_fare, rate_per_mile, payment_src_id, payment_type, payment_datetime,
           NULL AS customer_src_id, NULL AS customer_type, NULL AS customer_telephone, NULL AS promo_src_id, NULL AS promo_code,
           NULL AS discount_percentage, source_system, source_entity
    FROM sa_yellow_taxi.src_yellow_trip
) AS combined_data
GROUP BY trip_src_id;


--delete 
DELETE FROM bl_cl.combined_taxi_data
WHERE pickup_datetime > dropoff_datetime
OR (pickup_datetime IS NULL AND dropoff_datetime IS NOT NULL)
OR (dropoff_datetime IS NULL AND pickup_datetime IS NOT NULL);

--------------------------------------------------------------------------------------


-- here result one combined table without duplicates by trip_src_id column
select *
from bl_cl.combined_taxi_data;


--select count(*) from bl_cl.combined_taxi_data;
--drop table bl_cl.combined_taxi_data;

--check what we got in tables
--select *
--from sa_yellow_taxi.src_yellow_trip;

--select *
--from sa_yellow_taxi.ext_yellow_trip;

--DROP SERVER green_trip_file_server cascade;
--drop table sa_yellow_taxi.src_yellow_trip;

--select *
--from sa_green_taxi.src_green_trip where trip_src_id is null;

--select count(*) from sa_green_taxi.src_green_trip;
--drop table sa_green_taxi.src_green_trip;
--drop table sa_green_taxi.ext_green_trip;

