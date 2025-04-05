begin;

--Create a physical database

--create database nyc_taxi;


--create schema 
--DROP SCHEMA IF EXISTS bl_3nf CASCADE;

CREATE schema if not exists bl_3nf;

--1 Create first table with PK without FK
create table if not exists bl_3nf.ce_promotions (
    promo_id            bigint primary key,
    promo_code          varchar(255)  not null,
    discount_percentage integer       not null,
    promo_src_id        varchar(255)  not null,
    source_system       varchar(255)  not null,
    source_entity       varchar(255)  not null,
    update_dt           timestamp     not null,
    insert_dt           timestamp     not null
);


create table if not exists bl_3nf.ce_bookings (
    booking_id        bigint primary key,
    booking_type      varchar(255)      not null,
    booking_datetime  timestamp         null,
    booking_src_id    varchar(255)      not null,
    source_system     varchar(255)      not null,
    source_entity     varchar(255)      not null,
    update_dt         timestamp         not null,
    insert_dt         timestamp         not null
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
    insert_dt          timestamp    not null,
    constraint pk_ce_customers_scd primary key (customer_id, start_dt)
);

--drop table bl_3nf.ce_customers_scd;
	
create table if not exists bl_3nf.ce_payments (
    payment_id       bigint primary key,
    payment_type     varchar(255)      not null,
    payment_datetime timestamp         null,
    payment_src_id   varchar(255)      not null,
    source_system    varchar(255)      not null,
    source_entity    varchar(255)      not null,
    update_dt        timestamp         not null,
    insert_dt        timestamp         not null
);

create table if not exists bl_3nf.ce_rates (
    rate_id         bigint primary key,
    base_fare       decimal(10,2)  not null,
    rate_per_mile   decimal(10,2)  not null,
    rate_src_id     varchar(255)   not null,
    source_system   varchar(255)   not null,
    source_entity   varchar(255)   not null,
    update_dt       timestamp      not null,
    insert_dt       timestamp      not null
);

create table if not exists bl_3nf.ce_vendor_addresses (
    vendor_address_id      bigint primary key,                            --POPRAVKA s varchar na Bigint !!!
    vendor_street          varchar(255)      not null,
    vendor_house           varchar(255)      not null,
    vendor_city            varchar(255)      not null,
    vendor_country         varchar(255)      not null,
    vendor_postal_code     varchar(255)      not null,
    vendor_address_src_id  varchar(255)      not null,
    source_system          varchar(255)      not null,
    source_entity          varchar(255)      not null,
    update_dt              timestamp         not null,
    insert_dt              timestamp         not null
);
--drop table bl_3nf.ce_vendor_addresses cascade;

create table if not exists bl_3nf.ce_locations (
    location_id      bigint primary key,
    longitude        decimal(10,2)  not null,
    latitude         decimal(10,2)  not null,
    location_src_id  varchar(255)   not null,                            --POPRAVKA na varchar !!!
    source_system    varchar(255)   not null,
    source_entity    varchar(255)   not null,
    update_dt        timestamp      not null,
    insert_dt        timestamp      not null
);

--drop table bl_3nf.ce_locations cascade;


--2 create tables with PK and/or FK (referenced on created tables)
create table if not exists bl_3nf.ce_vendors (
    vendor_id         bigint primary key,
    vendor_address_id bigint            not null,
    vendor_name       varchar(255)      not null,
    vendor_telephone  varchar(255)      not null,
    vendor_src_id     varchar(255)      not null,  -- source_id
    source_system     varchar(255)      not null,
    source_entity     varchar(255)      not null,
    update_dt         timestamp         not null,
    insert_dt         timestamp         not null,
    constraint fk_ce_vendor_addresses_vendor foreign key (vendor_address_id) references bl_3nf.ce_vendor_addresses(vendor_address_id)
);
--drop table bl_3nf.ce_vendors cascade;


create table if not exists bl_3nf.ce_taxi_trips (
    trip_id               bigint primary key,                            --POPRAVKA s varchar na bigint !!!
    vendor_id             bigint         not null,
    booking_id            bigint         null,
    customer_id           bigint         not null,
    promo_id              bigint         null,
    payment_id            bigint         not null,
    rate_id               bigint         not null,
    pickup_location_id    bigint         not null,
    dropoff_location_id   bigint         not null,
    pickup_datetime       timestamp      null,
    dropoff_datetime      timestamp      null,
    distance_miles        decimal(10,2)  not null check (distance_miles >= 0),
    trip_duration         int            not null,
    passenger_count       int            not null,
    trip_src_id           varchar(255)   not null,
    source_system         varchar(255)   not null,
    source_entity         varchar(255)   not null,
    customer_start_dt     timestamp      not null,
    update_dt             timestamp      not null,
    insert_dt             timestamp      not null,
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
--------------------------------------------------------------------------------
--Modify combined_taxi_data table. Rename cell if inside null in bl_cl.combined_taxi_data


commit;


--select * from bl_3nf.ce_taxi_trips;