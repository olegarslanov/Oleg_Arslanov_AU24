--create schema
create schema if not exists bl_dm;

--begin transaction (only for DDL/DML)
begin;


-- create dimension tables

create table if not exists bl_dm.dim_dates (
    date_sur_id     bigint primary key,
    calendar_date   date not null,
    day_of_week     varchar(50) not null,
    day             int not null check (day > 0),
    month           int not null check (month between 1 and 12),
    quarter         int not null check (quarter between 1 and 4),
    year            int not null check (year >= 0)
);

--drop table bl_dm.dim_dates cascade;


-- add default values
insert into bl_dm.dim_dates
values (-1, '1970-01-01', 'n.a.', 1, 1, 1, 1970)
on conflict do nothing;


-- code for automatic publicate of dates in dim_dates table

--insert into bl_dm.DIM_DATES (date_sur_id, calendar_date, day_of_week, day, month, quarter, year)
--select 
	--cast(extract (year from dates) *10000 + extract(month from dates) *100 + extract(day from dates) as bigint) as date_sur_id,
	--dates as calendar_date,
	--to_char(dates, 'day') as day_of_week,
	--extract(day from dates) as day,
	--extract(month from dates) as month,
	--extract (quarter from dates) as quarter,
	--extract (year from dates) as year
--from generate_series('2000-01-01'::date, '2029-12-31'::date, '1 day'::interval) as dates
--where not exists (
--select 1 from bl_dm.dim_dates where calendar_date = dates
--);


create table if not exists bl_dm.dim_customers_scd (
    customer_sur_id    bigint primary key,
    customer_type      varchar(255) not null,
    customer_telephone varchar(255) not null,
    customer_src_id    varchar(255) not null,
    source_system      varchar(255) not null,
    source_entity      varchar(255) not null,
    start_dt           timestamp not null,
    end_dt             timestamp not null,
    is_active          varchar(1) not null default 'y',
    insert_dt          timestamp not null default current_timestamp,
    constraint chk_dim_customers_scd_dates check (end_dt > start_dt)
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


-- create table of time 
create table if not exists bl_dm.dim_time (
	time_sur_id            bigint primary key,
	calendar_time          time not null,
	hour                   int not null check (hour between 0 and 23) ,
	minute                 int not null check (minute between 0 and 59)
);

--drop table bl_dm.dim_time cascade;

--add default values
insert into bl_dm.dim_time
values (-1, '00:00:00', 0, 0)
on conflict do nothing;


-- code for automatic publicate of dates interval in dim_time table

--insert into bl_dm.DIM_TIME (time_sur_id, calendar_time, hour, minute)
--select 
	--cast(extract(hour from time_ ) * 100 + extract(minute from time_) as bigint) as time_sur_id,
    --time_::time as calendar_time,  -- Convert back to 'time'
    --extract(hour from time_) as hour,
    --extract(minute from time_) as minute
--from generate_series(
    --'2000-01-01 00:00:00'::timestamp + (time_start - '00:00'::time), --here (time_start - '00:00'::time) we made interval, so we can add it to timestam value and get needed time from
    --'2000-01-01 00:00:00'::timestamp + (time_end - '00:00'::time), 
    --interval_minute
--) as time_
--where not exists (
    --select 1 from bl_dm.DIM_TIME t where t.calendar_time = time_::time
--);



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
	distance_miles        decimal(10,2) not null check (distance_miles >= 0),
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
);
	
	--add default values
insert into bl_dm.fct_taxi_trips
values (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0.00, 0.00, current_date, current_date)
on conflict do nothing;

-------------------------------------------------------------------
--Add additional constraints


--check dim_promotions discount percentage <= 100

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'dim_promotions'
		and constraint_name = 'bl_dm_check_value_0_100'
) 
then 
	alter table bl_dm.dim_promotions
	add constraint bl_dm_check_value_0_100 check (discount_percentage >= 0 and discount_percentage <= 100);
end if;
end $$;



--check dim_customers_scd end_dt <start_dt

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'dim_customers_scd'
		and constraint_name = 'bl_dm_check_start_end_date'
) 
then 
	alter table bl_dm.dim_customers_scd
	add constraint bl_dm_check_start_end_date check (end_dt > start_dt);
end if;
end $$;

--check ce_taxi_trips trip_duration > 0 and passenger_count > 0

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'fct_taxi_trips'
		and constraint_name = 'bl_dm_check_trip_duration'
) 
then 
	alter table bl_dm.fct_taxi_trips
	add constraint bl_dm_check_trip_duration check (trip_duration > 0 or trip_duration = -1);
end if;

if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'fct_taxi_trips'
		and constraint_name = 'bl_dm_check_passenger_count'
) 
then 
	alter table bl_dm.fct_taxi_trips
	add constraint bl_dm_check_passenger_count check (passenger_count > 0 or passenger_count = -1);
end if;
end $$;

----------------------------------------------------------------------------------------------------------
--counting trip_amount
--alter table bl_dm.fct_taxi_trips
--add column trip_amount decimal(10,2) generated always as 
    --(distance_miles * rate_per_mile + base_fare) stored;

-- I understood that next we need to take data from 3NF and insert in Dim ...
	
commit;

--rollback;





