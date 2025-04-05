--Create a physical database
--create database nyc_taxi;
begin;
--create schema 
--DROP SCHEMA IF EXISTS bl_3nf CASCADE;
CREATE schema if not exists bl_3nf;

--1 Create first table with PK without FK
create table if not exists bl_3nf.ce_promotions (
    promo_id            bigint primary key,
    promo_code          varchar(255)  not null,
    discount_percentage decimal(5, 2) not null,
    promo_src_id        varchar(255)  not null,
    source_system       varchar(255)  not null,
    source_entity       varchar(255)  not null,
    update_dt           date          not null,
    insert_dt           date          not null
);

create table if not exists bl_3nf.ce_bookings (
    booking_id        bigint primary key,
    booking_type      varchar(255) not null,
    booking_datetime  timestamp    not null,
    booking_src_id    varchar(255) not null,
    source_system     varchar(255) not null,
    source_entity     varchar(255) not null,
    update_dt         date         not null,
    insert_dt         date         not null
);
	
create table if not exists bl_3nf.ce_customers_scd (
    customer_id        bigint       not null,
    customer_type      varchar(255) not null,
    customer_telephone varchar(255) not null,
    is_active          boolean      not null,
    start_dt           timestamp    not null,
    end_dt             timestamp    not null,
    customer_src_id    varchar(255) not null,
    source_system      varchar(255) not null,
    source_entity      varchar(255) not null,
    insert_dt          date not null,
    constraint pk_ce_customers_scd primary key (customer_id, start_dt)
);

--drop table bl_3nf.ce_customers_scd;
	
create table if not exists bl_3nf.ce_payments (
    payment_id       bigint primary key,
    payment_type     varchar(255) not null,
    payment_datetime timestamp    not null,
    payment_src_id   varchar(255) not null,
    source_system    varchar(255) not null,
    source_entity    varchar(255) not null,
    update_dt        date         not null,
    insert_dt        date         not null
);

create table if not exists bl_3nf.ce_rates (
    rate_id         bigint primary key,
    base_fare       decimal(10,2)  not null,
    rate_per_mile   decimal(10,2)  not null,
    rate_src_id     varchar(255)   not null,
    source_system   varchar(255)   not null,
    source_entity   varchar(255)   not null,
    update_dt       date           not null,
    insert_dt       date           not null
);

create table if not exists bl_3nf.ce_vendor_addresses (
    vendor_address_id      varchar(255) primary key,
    vendor_street          varchar(255) not null,
    vendor_house           varchar(255) not null,
    vendor_city            varchar(255) not null,
    vendor_country         varchar(255) not null,
    vendor_postal_code     varchar(255) not null,
    vendor_address_src_id  varchar(255) not null,
    source_system          varchar(255) not null,
    source_entity          varchar(255) not null,
    update_dt              date         not null,
    insert_dt              date         not null
);
--drop table bl_3nf.ce_vendor_addresses cascade;

create table if not exists bl_3nf.ce_locations (
    location_id      bigint primary key,
    longitude        decimal(10,2)  not null,
    latitude         decimal(10,2)  not null,
    location_src_id  bigint         not null,
    source_system    varchar(255)   not null,
    source_entity    varchar(255)   not null,
    update_dt        date           not null,
    insert_dt        date           not null
);

--drop table bl_3nf.ce_locations cascade;


--2 create tables with PK and/or FK (referenced on created tables)
create table if not exists bl_3nf.ce_vendors (
    vendor_id         bigint primary key,
    vendor_address_id varchar(255) not null,
    vendor_name       varchar(255) not null,
    vendor_telephone  varchar(255) not null,
    vendor_src_id     varchar(255) not null,
    source_system     varchar(255) not null,
    source_entity     varchar(255) not null,
    update_dt         date         not null,
    insert_dt         date         not null,
    constraint fk_ce_vendor_addresses_vendor foreign key (vendor_address_id) references bl_3nf.ce_vendor_addresses(vendor_address_id)
);
--drop table bl_3nf.ce_vendors cascade;


create table if not exists bl_3nf.ce_taxi_trips (
    trip_id               varchar(255) primary key,
    vendor_id             bigint         not null,
    booking_id            bigint         null,
    customer_id           bigint         not null,
    promo_id              bigint         null,
    payment_id            bigint         not null,
    rate_id               bigint         not null,
    pickup_location_id    bigint         not null,
    dropoff_location_id   bigint         not null,
    pickup_datetime       timestamp      not null,
    dropoff_datetime      timestamp      not null,
    distance_miles        decimal(10,2)  not null check (distance_miles >= 0),
    trip_duration         int            not null,
    passenger_count       int            not null,
    trip_src_id           varchar(255)   not null,
    source_system         varchar(255)   not null,
    source_entity         varchar(255)   not null,
    customer_start_dt     timestamp      not null,
    update_dt             date           not null,
    insert_dt             date           not null,
    constraint chk_ce_taxi_trips_time CHECK (dropoff_datetime > pickup_datetime),
    constraint fk_ce_taxi_trips_vendor foreign key (vendor_id) references bl_3nf.ce_vendors(vendor_id),
    constraint fk_ce_taxi_trips_booking foreign key (booking_id) references bl_3nf.ce_bookings(booking_id),
    constraint fk_ce_taxi_trips_promo foreign key (promo_id) references bl_3nf.ce_promotions(promo_id),
    constraint fk_ce_taxi_trips_payment foreign key (payment_id) references bl_3nf.ce_payments(payment_id),
    constraint fk_ce_taxi_trips_rate foreign key (rate_id) references bl_3nf.ce_rates(rate_id),
    constraint fk_ce_taxi_trips_pickup_loc foreign key (pickup_location_id) references bl_3nf.ce_locations(location_id),
    constraint fk_ce_taxi_trips_dropoff_loc foreign key (dropoff_location_id) references bl_3nf.ce_locations(location_id)
);


--drop table bl_3nf.ce_taxi_trips;

--Add additional constraints


--check ce_promotions discount percentage <= 100

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'ce_promotions'
		and constraint_name = 'check_value_0_100'
) 
then 
	alter table bl_3nf.ce_promotions
	add constraint check_value_0_100 check (discount_percentage >= 0 and discount_percentage <= 100);
end if;
end $$;

--check ce_customers_scd end_dt <start_dt

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'ce_customers_scd'
		and constraint_name = 'check_start_end_date'
) 
then 
	alter table bl_3nf.ce_customers_scd
	add constraint check_start_end_date check (end_dt > start_dt);
end if;
end $$;

--check ce_taxi_trips trip_duration > 0 and passenger_count > 0

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'ce_taxi_trips'
		and constraint_name = 'check_trip_duration'
) 
then 
	alter table bl_3nf.ce_taxi_trips
	add constraint check_trip_duration check (trip_duration > 0);
end if;

if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'ce_taxi_trips'
		and constraint_name = 'check_passenger_count'
) 
then 
	alter table bl_3nf.ce_taxi_trips
	add constraint check_passenger_count check (passenger_count > 0);
end if;
end $$;


--------------------------------------------------------------------------------
--Modify table. Rename cell if inside null in bl_cl.combined_taxi_data

-- privozu k obshemu formatu daty
UPDATE bl_cl.combined_taxi_data
SET pickup_datetime = 
    CASE 
        WHEN pickup_datetime::TEXT ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' 
            THEN pickup_datetime::TIMESTAMP
        WHEN pickup_datetime::TEXT ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} (AM|PM)' 
            THEN TO_TIMESTAMP(pickup_datetime::TEXT, 'DD/MM/YYYY HH:MI:SS AM')
        WHEN pickup_datetime::TEXT ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}' 
            THEN TO_TIMESTAMP(pickup_datetime::TEXT, 'DD/MM/YYYY HH24:MI')
        ELSE NULL::TIMESTAMP
    END
WHERE pickup_datetime IS NOT NULL;



UPDATE bl_cl.combined_taxi_data
SET dropoff_datetime = 
    CASE 
        WHEN cast(dropoff_datetime as text) ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' 
            THEN dropoff_datetime::timestamp
        WHEN cast(dropoff_datetime as text) ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} (AM|PM)' 
            THEN TO_TIMESTAMP(cast(dropoff_datetime as text), 'DD/MM/YYYY HH:MI:SS AM')
        WHEN cast(dropoff_datetime as text) ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}' 
            THEN TO_TIMESTAMP(cast(dropoff_datetime as text), 'DD/MM/YYYY HH24:MI')
        ELSE NULL
    END
WHERE dropoff_datetime IS NOT NULL;


UPDATE bl_cl.combined_taxi_data
SET booking_datetime = 
    CASE 
        WHEN cast(booking_datetime as text) ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' 
            THEN booking_datetime::timestamp
        WHEN cast(booking_datetime as text) ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} (AM|PM)' 
            THEN TO_TIMESTAMP(cast(booking_datetime as text), 'DD/MM/YYYY HH:MI:SS AM')
        WHEN cast(booking_datetime as text) ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}' 
            THEN TO_TIMESTAMP(cast(booking_datetime as text), 'DD/MM/YYYY HH24:MI')
        ELSE NULL
    END
WHERE booking_datetime IS NOT NULL;


UPDATE bl_cl.combined_taxi_data
SET payment_datetime = 
    CASE 
        WHEN cast(payment_datetime as text) ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' 
            THEN payment_datetime::timestamp
        WHEN cast(payment_datetime as text) ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} (AM|PM)' 
            THEN TO_TIMESTAMP(cast(payment_datetime as text), 'DD/MM/YYYY HH:MI:SS AM')
        WHEN cast(payment_datetime as text) ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}' 
            THEN TO_TIMESTAMP(cast(payment_datetime as text), 'DD/MM/YYYY HH24:MI')
        ELSE NULL
    END
WHERE booking_datetime IS NOT NULL;


-- ustanavlivaju obshij forma datestyle
SET datestyle = 'DMY, ISO';

--privozhu daty k data type timestamp
ALTER TABLE bl_cl.combined_taxi_data
ALTER COLUMN pickup_datetime TYPE timestamp USING pickup_datetime::timestamp,
ALTER COLUMN dropoff_datetime TYPE timestamp USING dropoff_datetime::timestamp,
ALTER COLUMN payment_datetime TYPE timestamp USING payment_datetime::timestamp,
ALTER COLUMN booking_datetime TYPE timestamp USING booking_datetime::timestamp;

--update all NULL on 'n.a.'
update bl_cl.combined_taxi_data	
set 
    --payment_datetime = coalesce(payment_datetime, 'n.a.'),
    booking_src_id = coalesce(booking_src_id, 'n.a.'),
    booking_type = coalesce(booking_type, 'n.a.'),
    --booking_datetime = coalesce(booking_datetime, 'n.a.'),
    customer_src_id = coalesce(customer_src_id, 'n.a.'),
    customer_type = coalesce(customer_type, 'n.a.'),
    promo_src_id = coalesce(promo_src_id, 'n.a.')
where
	--payment_datetime is NULL
	booking_src_id is null
	or booking_type is null
	--or booking_datetime is null
	or customer_src_id is null
	or customer_type is null
	or promo_src_id = '0';


-- update string 'NULL' and NULL to 'n.a.'
UPDATE bl_cl.combined_taxi_data
SET 
    customer_telephone = COALESCE(NULLIF(customer_telephone, 'NULL'), 'n.a.'),
    promo_code = COALESCE(NULLIF(promo_code, 'NULL'), 'n.a.'),
    booking_type = COALESCE(NULLIF(booking_type, 'NULL'), 'n.a.'),
    payment_type = COALESCE(NULLIF(payment_type, 'NULL'), 'n.a.'),
    customer_type = COALESCE(NULLIF(customer_type, 'NULL'), 'n.a.')
WHERE 
    customer_telephone IS NULL OR customer_telephone = 'NULL' 
    OR promo_code IS NULL OR promo_code = 'NULL'
    OR booking_type IS NULL OR booking_type = 'NULL'
    OR payment_type IS NULL OR payment_type = 'NULL'
    OR customer_type IS NULL OR customer_type = 'NULL';

-- discount_percentage NULL to 0
UPDATE bl_cl.combined_taxi_data
SET discount_percentage = 0
WHERE discount_percentage IS NULL;

--update NULL in datetime
UPDATE bl_cl.combined_taxi_data
SET pickup_datetime = COALESCE(pickup_datetime, '1970-01-01 00:00:00'::timestamp),
    dropoff_datetime = COALESCE(dropoff_datetime, '1970-01-01 00:00:00'::timestamp),
    payment_datetime = COALESCE(payment_datetime, '1970-01-01 00:00:00'::timestamp),
    booking_datetime = COALESCE(booking_datetime, '1970-01-01 00:00:00'::timestamp)
WHERE pickup_datetime IS NULL 
   OR dropoff_datetime IS NULL 
   OR payment_datetime IS NULL 
   OR booking_datetime IS NULL;

-- others
UPDATE bl_cl.combined_taxi_data
SET 
    promo_code = COALESCE(NULLIF(promo_code, 'NULL'), 'n.a.'),
    promo_src_id = CASE WHEN promo_src_id = '0' THEN 'n.a.' ELSE promo_src_id END,
    customer_telephone = COALESCE(NULLIF(customer_telephone, 'NULL'), 'n.a.')
WHERE 
    promo_code = 'NULL' 
    OR promo_src_id = '0' 
    OR customer_telephone = 'NULL';


--select * from bl_cl.combined_taxi_data;
--select * from sa_green_taxi.src_green_trip;
--select * from sa_yellow_taxi.src_yellow_trip;
--select promo_code from bl_cl.combined_taxi_data
--where promo_code = 'n.a.';


--alter date types
alter table bl_cl.combined_taxi_data
alter column trip_src_id type varchar(255),
alter column vendor_src_id type varchar(255),
alter column vendor_name type varchar(255),
alter column vendor_address_src_id type varchar(255),
alter column street type varchar(255),
alter column house type varchar(255),
alter column city type varchar(255),
alter column country type varchar(255),
alter column postal_code type varchar(255),
alter column vendor_telephone type varchar(255),
alter column pickup_datetime type timestamp using pickup_datetime::timestamp,
alter column dropoff_datetime type timestamp using dropoff_datetime::timestamp,
alter column passenger_count type int using passenger_count::int,
alter column location_src_id type bigint using location_src_id::bigint,
alter column pickup_longitude type decimal(10,2) using pickup_longitude::decimal(10,2),
alter column pickup_latitude type decimal(10,2) using pickup_latitude::decimal(10,2),
alter column dropoff_longitude type decimal(10,2) using dropoff_longitude::decimal(10,2),
alter column dropoff_latitude type decimal(10,2) using dropoff_latitude::decimal(10,2),
alter column distance_miles type decimal(10,2) using distance_miles::decimal(10,2),
alter column trip_duration type int using trip_duration::int,
alter column booking_src_id type varchar(255),
alter column booking_type type varchar(255),
alter column booking_datetime type timestamp using booking_datetime::timestamp,
alter column rate_src_id type varchar(255),
alter column base_fare type decimal(10,2) using base_fare::decimal(10,2),
alter column rate_per_mile type decimal(10,2) using rate_per_mile::decimal(10,2),
alter column payment_src_id type varchar(255),
alter column payment_type type varchar(255),
alter column customer_src_id type varchar(255),
alter column customer_type type varchar(255),
alter column customer_telephone type varchar(255),
alter column promo_src_id type varchar(255),
alter column promo_code type varchar(255),
alter column discount_percentage type decimal(5,2) using discount_percentage::decimal(5,2);

--kakaja to oshibka s nesootvetstviem tipa vendor_address_id, 'n.a.' tut napriamuju pomenial
INSERT INTO bl_3nf.ce_vendor_addresses (
    vendor_address_id,
    vendor_street,
    vendor_house,
    vendor_city,
    vendor_country,
    vendor_postal_code,
    vendor_address_src_id,
    source_system,
    source_entity,
    update_dt,
    insert_dt
) VALUES (
    'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 
    'n.a.', 'default', 'default', current_date, current_date
)
ON CONFLICT (vendor_address_id) DO NOTHING;

-------------------------------------------------------------------------
-- default row in each table (without prefact)


INSERT INTO bl_3nf.ce_promotions 
VALUES (-1, 'n.a.', 0.00, 'n.a.', 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_bookings 
VALUES (-1, 'n.a.', current_timestamp, 'n.a.', 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_customers_scd 
VALUES (-1, 'n.a.', 'n.a.', TRUE, current_timestamp, '9999-12-31 23:59:59', 'n.a.', 'default', 'default', current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_payments 
VALUES (-1, 'n.a.', current_timestamp, 'n.a.', 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_rates 
VALUES (-1, 0.00, 0.00, 'n.a.', 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_vendors 
VALUES (-1, 'n.a.', 'n.a.', 'n.a.', 'n.a', 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_vendor_addresses 
VALUES ('n.a', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;


INSERT INTO bl_3nf.ce_locations 
VALUES (-1, 0.00, 0.00, -1, 'default', 'default', current_date, current_date)
ON CONFLICT DO NOTHING;

INSERT INTO bl_3nf.ce_taxi_trips 
VALUES (
    'n.a', -1, -1, -1, -1, -1, -1, 
    -1, -1, '1970-01-01 00:00:00', '1970-01-01 00:01:00',
    0.00, 1, 1, 'n.a.',
    'default', 'default', '1970-01-01 00:00:00', current_date, current_date
)
ON CONFLICT DO NOTHING;

commit;
--SELECT * FROM bl_3nf.ce_vendor_addresses WHERE vendor_address_id = 'n.a.';

