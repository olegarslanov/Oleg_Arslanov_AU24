begin;

--create schema
CREATE SCHEMA IF NOT EXISTS bl_dm;

-- DIM_VENDORS
CREATE table if not exists bl_dm.dim_vendors (
    vendor_sur_id BIGINT PRIMARY KEY,
    vendor_name VARCHAR(255) NOT NULL,
    vendor_street VARCHAR(255),
    vendor_house VARCHAR(255),
    vendor_city VARCHAR(255),
    vendor_country VARCHAR(255),
    vendor_postal_code VARCHAR(255),
    vendor_telephone VARCHAR(255),
    vendor_src_id                   bigint NOT null unique, 
    vendor_address_src_id           bigint,
    source_system VARCHAR(255) NOT NULL,
    source_entity VARCHAR(255) NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    insert_dt TIMESTAMP NOT NULL
);

drop table bl_dm.dim_vendors cascade;

-- DIM_CUSTOMERS_SCD
CREATE TABLE if not exists bl_dm.dim_customers_scd (
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

drop table bl_dm.dim_customers_scd cascade;

-- DIM_RATES
CREATE TABLE if not exists bl_dm.dim_rates (
    rate_sur_id BIGINT PRIMARY KEY,
    base_fare DECIMAL(10,2) NOT NULL,
    rate_per_mile DECIMAL(10,2) NOT NULL,
    rate_src_id                      bigint NOT null unique,                    
    source_system VARCHAR(255) NOT NULL,
    source_entity VARCHAR(255) NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    insert_dt TIMESTAMP NOT NULL
);

drop table bl_dm.dim_rates cascade;

-- DIM_PROMOTIONS
CREATE TABLE if not exists bl_dm.dim_promotions (
    promo_sur_id BIGINT PRIMARY KEY,
    promo_code VARCHAR(255) NOT NULL,
    discount_percentage integer NOT NULL,
    promo_src_id                     bigint NOT null unique,                   
    source_system VARCHAR(255) NOT NULL,
    source_entity VARCHAR(255) NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    insert_dt TIMESTAMP NOT NULL
);

drop table bl_dm.dim_promotions cascade;

-- DIM_DATES
CREATE TABLE if not exists bl_dm.dim_dates (
    date_sur_id BIGINT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    day_of_week VARCHAR(255) NOT NULL,
    day integer NOT NULL,
    month integer NOT NULL,
    quarter integer NOT NULL,
    year integer NOT NULL
);

drop table bl_dm.dim_dates cascade;

-- DIM_TIME
CREATE TABLE if not exists bl_dm.dim_time (
    time_sur_id BIGINT PRIMARY KEY,
    calendar_time TIME NOT NULL UNIQUE,
    hour integer NOT NULL,
    minute integer NOT NULL
);

drop table bl_dm.dim_time cascade;

-- DIM_LOCATIONS
CREATE TABLE if not exists bl_dm.dim_locations (
    location_sur_id BIGINT PRIMARY KEY,
    longitude DECIMAL(10,6) NOT NULL,
    latitude DECIMAL(10,6) NOT NULL,
    location_src_id                         bigint NOT NULL,
    source_system VARCHAR(255) NOT NULL,
    source_entity VARCHAR(255) NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    insert_dt TIMESTAMP NOT NULL
);

drop table bl_dm.dim_locations cascade;

-- DIM_JUNK_ATTRIBUTES
CREATE TABLE if not exists bl_dm.dim_junk_attributes (
    junk_sur_id BIGINT PRIMARY KEY,
    payment_type VARCHAR(255) NOT NULL,
    booking_type VARCHAR(255) NOT NULL,
    booking_src_id                            bigint NOT null,
    payment_src_id                            bigint unique NOT NULL,
    source_system VARCHAR(255) NOT NULL,
    source_entity VARCHAR(255) NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    insert_dt TIMESTAMP NOT NULL
);

drop table bl_dm.dim_junk_attributes cascade;

-- fct_taxi_trips
CREATE TABLE IF NOT EXISTS bl_dm.fct_taxi_trips (
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
);

drop table bl_dm.fct_taxi_trips cascade;


-- Indexes for quicker find
CREATE INDEX if not exists idx_dim_vendors_src_id ON bl_dm.dim_vendors(vendor_src_id);
CREATE INDEX if not exists idx_dim_customers_src_id ON bl_dm.dim_customers_scd(customer_src_id);
CREATE INDEX if not exists idx_dim_rates_src_id ON bl_dm.dim_rates(rate_src_id);
CREATE INDEX if not exists idx_dim_promotions_src_id ON bl_dm.dim_promotions(promo_src_id);
CREATE INDEX if not exists idx_dim_locations_src_id ON bl_dm.dim_locations(location_src_id);
--CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_taxi_trips_trip_src_id ON bl_dm.fct_taxi_trips (trip_src_id);

--CREATE INDEX if not exists idx_dim_fct_taxi_trips ON bl_dm.fct_taxi_trips(trip_src_id);

commit;
--rollback;