--create schema
create schema if not exists bl_dm;

--begin transaction (only for DDL/DML)
begin;


-- create dimension tables

create table if not exists bl_dm.dim_dates (
    date_sur_id     bigint primary key,
    calendar_date   date not null,
    day_of_week     varchar(50) not null,
    day             int not null,
    month           int not null,
    quarter         int not null,
    year            int not null
);

-- add default values
insert into bl_dm.dim_dates
values (-1, '1970-01-01', 'n.a.', 1, 1, 1, 1970)
on conflict do nothing;

--select * from bl_dm.dim_dates;

create table if not exists bl_dm.dim_customers_scd (
    customer_sur_id    bigint primary key,
    customer_type      varchar(255) not null,
    customer_telephone varchar(255) not null,
    customer_src_id    varchar(255) not null,
    source_system      varchar(255) not null,
    source_entity      varchar(255) not null,
    start_dt           timestamp not null,
    end_dt             timestamp not null,
    is_active          varchar(1) not null,
    insert_dt          timestamp not null
);

-- add default values
insert into bl_dm.dim_customers_scd 
values (-1, 'n.a.', 'n.a.', 'n.a.', 'default', 'default', '1970-01-01 00:00:00', '9999-12-31 23:59:59', 'y', current_date)
on conflict do nothing;


create table if not exists bl_dm.dim_vendors (
    vendor_sur_id        bigint primary key,
    vendor_name          varchar(255) not null,
    vendor_street        varchar(255) not null,
    vendor_house         varchar(255) not null,
    vendor_city          varchar(255) not null,
    vendor_country       varchar(255) not null,
    vendor_postal_code   varchar(255) not null,
    vendor_telephone     varchar(255) not null,
    vendor_src_id        varchar(255) not null,
    vendor_address_src_id varchar(255) not null,
    source_system        varchar(255) not null,
    source_entity        varchar(255) not null,
    update_dt            timestamp not null,
    insert_dt            timestamp not null
);

-- add default values
insert into bl_dm.dim_vendors 
values (-1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'default', 'default', current_date, current_date)
on conflict do nothing;


create table if not exists bl_dm.dim_rates (
    rate_sur_id     bigint primary key,
    base_fare       decimal(10,2) not null,
    rate_per_mile   decimal(10,2) not null,
    rate_src_id     varchar(255) not null,
    source_system   varchar(255) not null,
    source_entity   varchar(255) not null,
    update_dt       timestamp not null,
    insert_dt       timestamp not null
);

-- add default values
insert into bl_dm.dim_rates 
values (-1, 0.00, 0.00, 'n.a.', 'default', 'default', current_date, current_date)
on conflict do nothing;


create table if not exists bl_dm.dim_locations (
    location_sur_id bigint primary key,
    longitude       decimal(10,6) not null,
    latitude        decimal(10,6) not null,
    location_src_id bigint not null,
    source_system   varchar(255) not null,
    source_entity   varchar(255) not null,
    update_dt       timestamp not null,
    insert_dt       timestamp not null
);

-- add default row
insert into bl_dm.dim_locations
values (-1, 0.000000, 0.000000, -1, 'default', 'default', current_date, current_date)
on conflict do nothing;


create table if not exists bl_dm.dim_promotions (
    promo_sur_id          bigint primary key,
    promo_code            varchar(255) not null,
    discount_percentage   decimal(5,2) not null,
    promo_src_id          varchar(255) not null,
    source_system         varchar(255) not null,
    source_entity         varchar(255) not null,
    update_dt             timestamp not null,
    insert_dt             timestamp not null
);

-- add default values
insert into bl_dm.dim_promotions 
values (-1, 'n.a.', 0.00, 'n.a.', 'default', 'default', current_date, current_date)
on conflict do nothing;


create table if not exists bl_dm.dim_time (
	time_sur_id            bigint primary key,
	calendar_time          time not null,
	hour                   int not null,
	minute                 int not null
);

--add default values
insert into bl_dm.dim_time
values (-1, '00:00:00', 0, 0)
on conflict do nothing;
	

create table if not exists bl_dm.dim_junk_attributes (
	junk_sur_id            bigint primary key,
	payment_type           varchar(255) not null,
	booking_type           varchar(255) not null,
	booking_src_id         varchar(255) not null,
	payment_src_id         varchar(255) not null,
	source_system          varchar(255) not null,
	source_entity          varchar(255) not null,
	update_dt              timestamp not null,
	insert_dt              timestamp not null
);
	
	
--add default values
insert into bl_dm.dim_junk_attributes
values (-1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', current_date, current_date)
on conflict do nothing;


-- create fact table
create table if not exists bl_dm.fct_taxi_trips (
	vendor_sur_id         bigint not null,
	customer_sur_id       bigint not null,
	junk_sur_id           bigint not null,
	rate_sur_id           bigint not null,
	promo_sur_id          bigint not null,
	pickup_location_id    bigint not null,
	dropoff_location_id   bigint not null,
	booking_date_id       bigint not null,
	pickup_date_id        bigint not null,
	dropoff_date_id       bigint not null,
	payment_date_id       bigint not null,
	pickup_time_id        bigint not null, 
    dropoff_time_id       bigint not null, 
    booking_time_id       bigint not null, 
    payment_time_id       bigint not null, 
	trip_duration         integer not null,
	passenger_count       integer not null,
	distance_miles        decimal(10,2) not null,
	trip_amount           decimal(10,2) not null,
	update_dt             timestamp not null,
	insert_dt             timestamp not null,
	constraint fk_dim_taxi_trips_vendor foreign key (vendor_sur_id) references bl_dm.dim_vendors(vendor_sur_id),
	constraint fk_dim_taxi_trips_customer foreign key (customer_sur_id) references bl_dm.dim_customers_scd(customer_sur_id),
	constraint fk_dim_taxi_trips_junk foreign key (junk_sur_id) references bl_dm.dim_junk_attributes(junk_sur_id),
	constraint fk_dim_taxi_trips_rate foreign key (rate_sur_id) references bl_dm.dim_rates(rate_sur_id),
	constraint fk_dim_taxi_trips_promo foreign key (promo_sur_id) references bl_dm.dim_promotions(promo_sur_id),
	constraint fk_dim_taxi_trips_pickup_location foreign key (pickup_location_id) references bl_dm.dim_locations(location_sur_id),
	constraint fk_dim_taxi_trips_dropoff_location foreign key (dropoff_location_id) references bl_dm.dim_locations(location_sur_id),
	-----
	constraint fk_dim_taxi_trips_booking_date foreign key (booking_date_id) references bl_dm.dim_dates(date_sur_id),
	constraint fk_dim_taxi_trips_pickup_date foreign key (pickup_date_id) references bl_dm.dim_dates(date_sur_id),
	constraint fk_dim_taxi_trips_dropoff_date foreign key (dropoff_date_id) references bl_dm.dim_dates(date_sur_id),
	constraint fk_dim_taxi_trips_payment_date foreign key (payment_date_id) references bl_dm.dim_dates(date_sur_id),
	-----
	constraint fk_dim_taxi_trips_booking_time foreign key (booking_time_id) references bl_dm.dim_time(time_sur_id),
	constraint fk_dim_taxi_trips_pickup_time foreign key (pickup_time_id) references bl_dm.dim_time(time_sur_id),
	constraint fk_dim_taxi_trips_dropoff_time foreign key (dropoff_time_id) references bl_dm.dim_time(time_sur_id),
	constraint fk_dim_taxi_trips_payment_time foreign key (payment_time_id) references bl_dm.dim_time(time_sur_id)
	------
	--constraint fk_dim_taxi_trips_trip_duration foreign key (trip_duration) references bl_3nf.ce_taxi_trips(trip_duration),
	--constraint fk_dim_taxi_trips_passenger_count foreign key (passenger_count) references bl_3nf.ce_taxi_trips(passenger_count),
	--constraint fk_dim_taxi_trips_distance_miles foreign key (distance_miles) references bl_3nf.ce_taxi_trips(distance_miles)
);
	
	--add default values
insert into bl_dm.fct_taxi_trips
values (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0.00, 0.00, current_date, current_date)
on conflict do nothing;

	
--finish transaction	
commit;
--rollback;
	
	
--select type(trip_duration) from bl_cl.combined_taxi_data ctd; 




--drop schema bl_dm cascade;

--select * from bl_dm.dim_vendors;





