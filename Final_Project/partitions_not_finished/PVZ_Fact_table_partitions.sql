--DIM table 


-- 1. create parental table with partition (this table dont hold data, this table is only logical point for child table data ... only children tables hold data)

CREATE TABLE IF NOT EXISTS bl_dm.fct_taxi_trips_partition (
    trip_sur_id          BIGINT,  
    vendor_sur_id        BIGINT NOT NULL,        
    customer_sur_id      BIGINT NOT NULL,                 
    junk_sur_id          BIGINT NOT NULL,                 
    rate_sur_id          BIGINT NOT NULL,               
    promo_sur_id         BIGINT NOT NULL,              
    pickup_location_id   BIGINT NOT NULL,             
    dropoff_location_id  BIGINT NOT NULL, 
    booking_date_id      BIGINT, 
    pickup_date_id       BIGINT,             
    dropoff_date_id      BIGINT, 
    payment_date_id      BIGINT,  
    pickup_time_id       BIGINT,            
    dropoff_time_id      BIGINT, 
    booking_time_id      BIGINT, 
    payment_time_id      BIGINT,
    trip_src_id          BIGINT NOT NULL,
    trip_id              VARCHAR(255) NOT NULL,
    trip_duration        INT NOT NULL, 
    passenger_count      INT NOT NULL, 
    distance_miles       DECIMAL(10,2) NOT NULL, 
    trip_amount          DECIMAL(10,2) NOT NULL,            
    update_dt            TIMESTAMP NOT NULL, 
    insert_dt            TIMESTAMP NOT NULL,
    pickup_datetime      TIMESTAMP                 -- column for partition
) PARTITION BY RANGE (pickup_datetime);            --here we add this syntax for can be using partitions


--2. create partition table
CREATE table if not EXISTS bl_dm.fct_taxi_trips_2016_01_03 (
    trip_sur_id          BIGINT,  
    vendor_sur_id        BIGINT NOT NULL,        
    customer_sur_id      BIGINT NOT NULL,                 
    junk_sur_id          BIGINT NOT NULL,                 
    rate_sur_id          BIGINT NOT NULL,               
    promo_sur_id         BIGINT NOT NULL,              
    pickup_location_id   BIGINT NOT NULL,             
    dropoff_location_id  BIGINT NOT NULL, 
    booking_date_id      BIGINT, 
    pickup_date_id       BIGINT,             
    dropoff_date_id      BIGINT, 
    payment_date_id      BIGINT,  
    pickup_time_id       BIGINT,            
    dropoff_time_id      BIGINT, 
    booking_time_id      BIGINT, 
    payment_time_id      BIGINT,
    trip_src_id          BIGINT NOT NULL,
    trip_id              VARCHAR(255) NOT NULL,
    trip_duration        INT NOT NULL, 
    passenger_count      INT NOT NULL, 
    distance_miles       DECIMAL(10,2) NOT NULL, 
    trip_amount          DECIMAL(10,2) NOT NULL,            
    update_dt            TIMESTAMP NOT NULL, 
    insert_dt            TIMESTAMP NOT NULL,
    pickup_datetime      TIMESTAMP
);


--3. atach created partition to parental table
ALTER TABLE bl_dm.fct_taxi_trips_partition 
    ATTACH PARTITION bl_dm.fct_taxi_trips_2016_01_03 
    FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2016-03-31 00:00:00');

-- optional if I want change period of time ...
ALTER TABLE bl_dm.fct_taxi_trips_partition 
	DETACH PARTITION bl_dm.fct_taxi_trips_2016_01_03_new;

CREATE table if not exists bl_dm.fct_taxi_trips_2016_02_03_new (
    trip_sur_id          BIGINT not null,  
    vendor_sur_id        BIGINT NOT NULL,        
    customer_sur_id      BIGINT NOT NULL,                 
    junk_sur_id          BIGINT NOT NULL,                 
    rate_sur_id          BIGINT NOT NULL,               
    promo_sur_id         BIGINT NOT NULL,              
    pickup_location_id   BIGINT NOT NULL,             
    dropoff_location_id  BIGINT NOT NULL, 
    booking_date_id      BIGINT, 
    pickup_date_id       BIGINT,             
    dropoff_date_id      BIGINT, 
    payment_date_id      BIGINT,  
    pickup_time_id       BIGINT,            
    dropoff_time_id      BIGINT, 
    booking_time_id      BIGINT, 
    payment_time_id      BIGINT,
    trip_src_id          BIGINT NOT NULL,
    trip_id              VARCHAR(255) NOT NULL,
    trip_duration        INT NOT NULL, 
    passenger_count      INT NOT NULL, 
    distance_miles       DECIMAL(10,2) NOT NULL, 
    trip_amount          DECIMAL(10,2) NOT NULL,            
    update_dt            TIMESTAMP NOT NULL, 
    insert_dt            TIMESTAMP NOT NULL,
    pickup_datetime      TIMESTAMP not null
);

--drop table bl_dm.fct_taxi_trips_2016_01_03_new;

ALTER TABLE bl_dm.fct_taxi_trips_partition 
    ATTACH PARTITION bl_dm.fct_taxi_trips_2016_02_03_new 
    FOR VALUES FROM ('2016-02-01 00:00:00') TO ('2016-04-01 00:00:00');


--4. atach complex primary key to parental table
ALTER table bl_dm.fct_taxi_trips_partition
    ADD CONSTRAINT pk_fct_taxi_trips_partition 
    PRIMARY KEY (trip_sur_id, pickup_datetime);


--5.

-- Create sequence for surrogate keys
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_fct_taxi_trips_partition;

--create unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_taxi_trips_datetime_trip_src_id
ON bl_dm.fct_taxi_trips_partition (trip_src_id, pickup_datetime);

--DROP INDEX IF EXISTS idx_fct_taxi_trips_trip_src_id;

--create procedure
CREATE OR REPLACE PROCEDURE bl_dm.load_fct_taxi_trips_partition()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time     TIMESTAMP := now();
    v_rows_affected  INT;
BEGIN
    ----------------------------------------------------------------
    -- 1. Start info to log
    ----------------------------------------------------------------
    INSERT INTO bl_dm.load_logs (procedure_name, start_time, status)
    VALUES ('load_fct_taxi_trips_partition', v_start_time, 'STARTED');

    ----------------------------------------------------------------
    -- 2. Insert
    ----------------------------------------------------------------
    EXECUTE '
        INSERT INTO bl_dm.fct_taxi_trips_partition (
            trip_sur_id,
            vendor_sur_id,
            customer_sur_id,
            junk_sur_id,
            rate_sur_id,
            promo_sur_id,
            pickup_location_id,
            dropoff_location_id,
			booking_date_id,
            pickup_date_id,
            dropoff_date_id,
			payment_date_id,
            pickup_time_id,
            dropoff_time_id,
			booking_time_id,
			payment_time_id,
			trip_src_id,
			trip_id,
			trip_duration,
            passenger_count,
			distance_miles,
			trip_amount,
            update_dt,
            insert_dt,
			pickup_datetime
        )
        SELECT distinct
			nextval (''bl_dm.seq_fct_taxi_trips_partition''),
            dv.vendor_sur_id,
			dcs.customer_sur_id,
			dja.junk_sur_id,
			dr.rate_sur_id,
			dp.promo_sur_id,
			dlp.location_sur_id,
			dld.location_sur_id,
			ddb.date_sur_id as booking_date_id,                                                   
			ddp.date_sur_id AS pickup_date_id,
			ddd.date_sur_id AS dropoff_date_id,   
			ddpy.date_sur_id as payment_date_id,
			dtp.time_sur_id AS pickup_time_id,
			dtd.time_sur_id AS dropoff_time_id,
			dtb.time_sur_id AS booking_time_id,
			dtpy.time_sur_id AS payment_time_id,
			ctt.trip_id,
			ctt.trip_src_id,
			ctt.trip_duration,
			ctt.passenger_count,
			ctt.distance_miles,
			dr.base_fare + (ctt.distance_miles * dr.rate_per_mile) as trip_amount,
            now() AS update_dt,
            now() AS insert_dt,
			ctt.pickup_datetime
FROM 
			bl_3nf.ce_taxi_trips ctt
        	left join bl_dm.dim_vendors dv 
				on dv.vendor_src_id = ctt.vendor_id
			left join bl_dm.dim_customers_scd dcs
				on dcs.customer_src_id = ctt.customer_id
				and dcs.is_active = true
			left join bl_dm.dim_junk_attributes dja
				on dja.payment_src_id = ctt.payment_id
				and dja.booking_src_id = ctt.booking_id
			left join bl_dm.dim_rates dr
				on dr.rate_src_id = ctt.rate_id
			left join bl_dm.dim_promotions dp
				on dp.promo_src_id = ctt.promo_id
			LEFT JOIN bl_dm.dim_locations dlp
                ON dlp.location_src_id = ctt.pickup_location_id
        	LEFT JOIN bl_dm.dim_locations dld
               ON dld.location_src_id = ctt.dropoff_location_id		       
	 		LEFT JOIN bl_dm.dim_dates ddb
	               ON ddb.calendar_date = (select date(cb.booking_datetime)
										   from bl_3nf.ce_bookings cb
										   where ctt.booking_id = cb.booking_id limit 1) 
	        LEFT JOIN bl_dm.dim_dates ddp
	               ON ddp.calendar_date = DATE(ctt.pickup_datetime)        
	        LEFT JOIN bl_dm.dim_dates ddd
	               ON ddd.calendar_date = DATE(ctt.dropoff_datetime)      		
			LEFT JOIN bl_dm.dim_dates ddpy
	               ON ddpy.calendar_date = (select date(cpy.payment_datetime)
										   from bl_3nf.ce_payments cpy
										   where ctt.payment_id = cpy.payment_id limit 1)
			LEFT JOIN bl_dm.dim_time dtp
                ON dtp.calendar_time = CAST(ctt.pickup_datetime AS time)
        	LEFT JOIN bl_dm.dim_time dtd
            	ON dtd.calendar_time = CAST(ctt.dropoff_datetime AS time)
			LEFT JOIN bl_dm.dim_time dtb
	            ON dtb.calendar_time = (select CAST(cb.booking_datetime as time)
										   from bl_3nf.ce_bookings cb
										   where ctt.booking_id = cb.booking_id limit 1) 
            LEFT JOIN bl_dm.dim_time dtpy
	            ON dtpy.calendar_time = (select CAST(cpy.payment_datetime as time)
										   from bl_3nf.ce_payments cpy
										   where ctt.payment_id = cpy.payment_id limit 1)
       WHERE ctt.pickup_datetime BETWEEN (''2016-04-01 00:00:00''::timestamp - interval ''2 months'')
              AND ''2016-04-01 00:00:00''::timestamp
	        ON CONFLICT (trip_src_id, pickup_datetime)
	        DO UPDATE
	        SET
			    trip_duration   = EXCLUDED.trip_duration,
			    passenger_count = EXCLUDED.passenger_count,
			    distance_miles  = EXCLUDED.distance_miles,
			    trip_amount     = EXCLUDED.trip_amount,
			    update_dt       = now()
			WHERE
			    fct_taxi_trips_partition.distance_miles IS DISTINCT FROM EXCLUDED.distance_miles
			    OR fct_taxi_trips_partition.trip_duration IS DISTINCT FROM EXCLUDED.trip_duration
			    OR fct_taxi_trips_partition.passenger_count IS DISTINCT FROM EXCLUDED.passenger_count;
';
    ----------------------------------------------------------------
    -- 3. got how many rows are affected
    ----------------------------------------------------------------
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    ----------------------------------------------------------------
    -- 4. End of procedure to log
    ----------------------------------------------------------------
    INSERT INTO bl_dm.load_logs (procedure_name, start_time, end_time, rows_affected, status)
    VALUES ('load_fct_taxi_trips_partition', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- end notice
    RAISE NOTICE 'Procedure load_fct_taxi_trips_partition completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- error log
        INSERT INTO bl_dm.load_logs (procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES ('load_fct_taxi_trips_partition', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


  
--Testing     

CALL bl_dm.load_fct_taxi_trips_partition();

        
SELECT * FROM bl_dm.fct_taxi_trips;
SELECT * FROM bl_dm.fct_taxi_trips_partition;
SELECT * FROM bl_dm.fct_taxi_trips_2016_02_03_new;

truncate table bl_dm.fct_taxi_trips;
truncate table bl_dm.fct_taxi_trips_partition;


--query for looking how many rows are affected
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_fct_taxi_trips_partition';

-- quantity of unique trip_src_id in source       ------995 rows 
SELECT COUNT(DISTINCT trip_src_id) AS source_keys
FROM bl_3nf.ce_taxi_trips;

-- quantity unique trip_src_id in fact table DM   ----- 321 rows
SELECT COUNT(DISTINCT trip_src_id) AS dm_keys
FROM bl_dm.fct_taxi_trips_partition;






-----------------------------------------------------------------------
-- Here I do 'partition' with adding where filtr :)

-- Create sequence for surrogate keys
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_fct_taxi_trips;

--create unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_taxi_trips_trip_src_id
ON bl_dm.fct_taxi_trips (trip_src_id);


--create function
CREATE OR REPLACE PROCEDURE bl_dm.load_fct_taxi_trips()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time     TIMESTAMP := now();
    v_rows_affected  INT;
BEGIN
    ----------------------------------------------------------------
    -- 1. Start info to log
    ----------------------------------------------------------------
    INSERT INTO bl_dm.load_logs (procedure_name, start_time, status)
    VALUES ('load_fct_taxi_trips', v_start_time, 'STARTED');

    ----------------------------------------------------------------
    -- 2. Insert
    ----------------------------------------------------------------
    EXECUTE '
        INSERT INTO bl_dm.fct_taxi_trips (
            trip_sur_id,
            vendor_sur_id,
            customer_sur_id,
            junk_sur_id,
            rate_sur_id,
            promo_sur_id,
            pickup_location_id,
            dropoff_location_id,
			booking_date_id,
            pickup_date_id,
            dropoff_date_id,
			payment_date_id,
            pickup_time_id,
            dropoff_time_id,
			booking_time_id,
			payment_time_id,
			trip_src_id,
			trip_id,
			trip_duration,
            passenger_count,
			distance_miles,
			trip_amount,
            update_dt,
            insert_dt
        )
        SELECT distinct
			nextval (''bl_dm.seq_fct_taxi_trips''),
            dv.vendor_sur_id,
			dcs.customer_sur_id,
			dja.junk_sur_id,
			dr.rate_sur_id,
			dp.promo_sur_id,
			dlp.location_sur_id,
			dld.location_sur_id,
			
			ddb.date_sur_id as booking_date_id,                                                   
			ddp.date_sur_id AS pickup_date_id,
			ddd.date_sur_id AS dropoff_date_id,   
			ddpy.date_sur_id as payment_date_id,
			
			dtp.time_sur_id AS pickup_time_id,
			dtd.time_sur_id AS dropoff_time_id,
			dtb.time_sur_id AS booking_time_id,
			dtpy.time_sur_id AS payment_time_id,
			
			ctt.trip_id,
			ctt.trip_src_id,
			ctt.trip_duration,
			ctt.passenger_count,
			ctt.distance_miles,
			dr.base_fare + (ctt.distance_miles * dr.rate_per_mile) as trip_amount,
            now() AS update_dt,
            now() AS insert_dt


        FROM 
			bl_3nf.ce_taxi_trips ctt
        	left join bl_dm.dim_vendors dv 
				on dv.vendor_src_id = ctt.vendor_id
			left join bl_dm.dim_customers_scd dcs
				on dcs.customer_src_id = ctt.customer_id
				and dcs.is_active = true
			left join bl_dm.dim_junk_attributes dja
				on dja.payment_src_id = ctt.payment_id
				and dja.booking_src_id = ctt.booking_id
			left join bl_dm.dim_rates dr
				on dr.rate_src_id = ctt.rate_id
			left join bl_dm.dim_promotions dp
				on dp.promo_src_id = ctt.promo_id
			LEFT JOIN bl_dm.dim_locations dlp
                ON dlp.location_src_id = ctt.pickup_location_id
        	LEFT JOIN bl_dm.dim_locations dld
               ON dld.location_src_id = ctt.dropoff_location_id		   
	        
	 		LEFT JOIN bl_dm.dim_dates ddb
	               ON ddb.calendar_date = (select date(cb.booking_datetime)
										   from bl_3nf.ce_bookings cb
										   where ctt.booking_id = cb.booking_id limit 1) 
	        LEFT JOIN bl_dm.dim_dates ddp
	               ON ddp.calendar_date = DATE(ctt.pickup_datetime)        
	        LEFT JOIN bl_dm.dim_dates ddd
	               ON ddd.calendar_date = DATE(ctt.dropoff_datetime)      		
			LEFT JOIN bl_dm.dim_dates ddpy
	               ON ddpy.calendar_date = (select date(cpy.payment_datetime)
										   from bl_3nf.ce_payments cpy
										   where ctt.payment_id = cpy.payment_id limit 1)
         	
			LEFT JOIN bl_dm.dim_time dtp
                ON dtp.calendar_time = CAST(ctt.pickup_datetime AS time)
        	LEFT JOIN bl_dm.dim_time dtd
            	ON dtd.calendar_time = CAST(ctt.dropoff_datetime AS time)
			LEFT JOIN bl_dm.dim_time dtb
	            ON dtb.calendar_time = (select CAST(cb.booking_datetime as time)
										   from bl_3nf.ce_bookings cb
										   where ctt.booking_id = cb.booking_id limit 1) 
            LEFT JOIN bl_dm.dim_time dtpy
	            ON dtpy.calendar_time = (select CAST(cpy.payment_datetime as time)
										   from bl_3nf.ce_payments cpy
										   where ctt.payment_id = cpy.payment_id limit 1)

      	WHERE ctt.pickup_datetime BETWEEN (''2016-04-01 00:00:00''::timestamp - interval ''2 months'') AND ''2016-04-01 00:00:00''::timestamp
        ON CONFLICT (trip_src_id)
        DO UPDATE
        SET
	        trip_duration         = EXCLUDED.trip_duration,
            passenger_count       = EXCLUDED.passenger_count,
			distance_miles        = EXCLUDED.distance_miles,
            trip_amount           = EXCLUDED.trip_amount,
            update_dt       = now()
			WHERE
			    fct_taxi_trips.distance_miles IS DISTINCT FROM EXCLUDED.distance_miles
			    OR fct_taxi_trips.trip_duration IS DISTINCT FROM EXCLUDED.trip_duration
			    OR fct_taxi_trips.passenger_count IS DISTINCT FROM EXCLUDED.passenger_count;
    ';

    ----------------------------------------------------------------
    -- 3. got how many rows are affected
    ----------------------------------------------------------------
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    ----------------------------------------------------------------
    -- 4. End of procedure to log
    ----------------------------------------------------------------
    INSERT INTO bl_dm.load_logs (procedure_name, start_time, end_time, rows_affected, status)
    VALUES ('load_fct_taxi_trips', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- end notice
    RAISE NOTICE 'Procedure load_fct_taxi_trips completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- error log
        INSERT INTO bl_dm.load_logs (procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES ('load_fct_taxi_trips', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;



--Testing

CALL bl_dm.load_fct_taxi_trips();

SELECT * FROM bl_dm.fct_taxi_trips;

truncate table bl_dm.fct_taxi_trips;


--query for looking how many rows are affected
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_fct_taxi_trips';

-- quantity of unique trip_src_id in source       ------995 rows 
SELECT COUNT(DISTINCT trip_src_id) AS source_keys
FROM bl_3nf.ce_taxi_trips;

-- quantity unique trip_src_id in fact table DM   ----- 321 rows
SELECT COUNT(DISTINCT trip_src_id) AS dm_keys
FROM bl_dm.fct_taxi_trips;







---------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------
--3NF

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
    distance_miles        DECIMAL(10,2)  NOT NULL CHECK (distance_miles >= 0),
    trip_duration         INT            NOT NULL,
    passenger_count       INT            NOT NULL,
    trip_src_id           VARCHAR(255)   NOT NULL,
    source_system         VARCHAR(255)   NOT NULL,
    source_entity         VARCHAR(255)   NOT NULL,
    customer_start_dt     TIMESTAMP      NOT NULL,
    update_dt             TIMESTAMP      NOT NULL,
    insert_dt             TIMESTAMP      NOT NULL,
    CONSTRAINT chk_ce_taxi_trips_partition_time CHECK (dropoff_datetime > pickup_datetime)
) PARTITION BY RANGE (pickup_datetime);

--drop table bl_3nf.ce_taxi_trips_partition;

--2. create partition table
CREATE TABLE IF NOT EXISTS bl_3nf.ce_taxi_trips_2016_01 (
    trip_id               BIGINT         NOT NULL,  
    vendor_id             BIGINT         NOT NULL,
    booking_id            BIGINT         NULL,
    customer_id           BIGINT         NOT NULL,
    promo_id              BIGINT         NULL,
    payment_id            BIGINT         NOT NULL,
    rate_id               BIGINT         NOT NULL,
    pickup_location_id    BIGINT         NOT NULL,
    dropoff_location_id   BIGINT         NOT NULL,
    pickup_datetime       TIMESTAMP      NOT NULL,
    dropoff_datetime      TIMESTAMP      NULL,
    distance_miles        DECIMAL(10,2)  NOT NULL CHECK (distance_miles >= 0),
    trip_duration         INT            NOT NULL,
    passenger_count       INT            NOT NULL,
    trip_src_id           VARCHAR(50)   NOT NULL,
    source_system         VARCHAR(255)   NOT NULL,
    source_entity         VARCHAR(255)   NOT NULL,
    customer_start_dt     TIMESTAMP      NOT NULL,
    update_dt             TIMESTAMP      NOT NULL,
    insert_dt             TIMESTAMP      NOT NULL,
    CONSTRAINT chk_ce_taxi_trips_partition_time CHECK (dropoff_datetime > pickup_datetime)
);

drop table bl_3nf.ce_taxi_trips_2016_01;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ce_taxi_trips_datetime_trip_src_id
ON bl_dm.fct_taxi_trips_partition (trip_src_id, pickup_datetime);



ALTER TABLE bl_3nf.ce_taxi_trips_2016_01
    ADD CONSTRAINT ce_taxi_trips_partition_distance_miles_check1 CHECK (distance_miles >= 0);

--3.  atach created partition to parental table
ALTER TABLE bl_3nf.ce_taxi_trips_partition 
    ATTACH PARTITION bl_3nf.ce_taxi_trips_2016_01 
    FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2016-01-01 23:59:59');





--ALTER TABLE bl_3nf.ce_taxi_trips_partition 
	--DETACH PARTITION bl_3nf.ce_taxi_trips_2016_02_03_new;

--SELECT * FROM pg_partition_tree('bl_3nf.ce_taxi_trips_partition');




--4. atach complex primary key to parental table
ALTER table bl_3nf.ce_taxi_trips_partition
    ADD CONSTRAINT pk_ce_taxi_trips_partition 
    PRIMARY KEY (trip_id, pickup_datetime);


-- ce_taxi_trips

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_taxi_trip_partition;


--create unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_taxi_trips_datetime_trip_src_id
ON bl_dm.fct_taxi_trips_partition (trip_id, pickup_datetime);



--func
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_taxi_trips_partition(load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
BEGIN
    -- Log start
    RAISE NOTICE 'Start of % load at %', load_type, v_start_time;

    -- Validate load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''FULL'' or ''INCREMENTAL''.', load_type;
    END IF;

    ----------------------------------------------------------------
	-- 1. FULL LOAD
	IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE 'Full load started at %', clock_timestamp();


	-- A) TRUNCATE FACT TABLE
	TRUNCATE TABLE bl_3nf.ce_taxi_trips CASCADE;
	
	-- B) insert values in table
	WITH -- I choose columns that I need it is faster way to fill table
	vendor_ids AS (
	    SELECT DISTINCT ON (vendor_src_id, vendor_telephone) vendor_src_id, vendor_id
	    FROM bl_3nf.ce_vendors
	),
	booking_ids AS (
	    SELECT DISTINCT ON (booking_src_id) booking_src_id, booking_id
	    FROM bl_3nf.ce_bookings
	),
	customer_ids AS (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_src_id, customer_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    WHERE is_active = true
	),
	promo_ids AS (
	    SELECT DISTINCT ON (promo_src_id) promo_src_id, promo_id
	    FROM bl_3nf.ce_promotions
	),
	payment_ids AS (
	    SELECT DISTINCT ON (payment_src_id) payment_src_id, payment_id
	    FROM bl_3nf.ce_payments
	),
	rate_ids AS (
	    SELECT DISTINCT ON (rate_src_id) rate_src_id, rate_id
	    FROM bl_3nf.ce_rates
	),
	pickup_locations AS (
	    SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	),
	dropoff_locations AS (
	    SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	)
	INSERT INTO bl_3nf.ce_taxi_trips_partition (
	    trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
	    pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
	    distance_miles, trip_duration, passenger_count, trip_src_id,
	    source_system, source_entity, customer_start_dt, update_dt, insert_dt
	)
	SELECT DISTINCT ON (ctd.trip_src_id)
	    nextval('bl_3nf.seq_ce_taxi_trip_partition') AS trip_id,                                     
	    COALESCE(v.vendor_id, -1) AS vendor_id, 
	    COALESCE(b.booking_id, -1) AS booking_id, 
	    COALESCE(c.customer_id, -1) AS customer_id, 
	    COALESCE(p.promo_id, -1) AS promo_id, 
	    COALESCE(py.payment_id, -1) AS payment_id, 
	    COALESCE(r.rate_id, -1) AS rate_id, 
	    COALESCE(pl.location_id, -1) AS pickup_location_id, 
	    COALESCE(dl.location_id, -1) AS dropoff_location_id, 
	    ctd.pickup_datetime,                                  
	    ctd.dropoff_datetime, 
	    COALESCE(ctd.distance_miles, 0.00) AS distance_miles, 
	    COALESCE(ctd.trip_duration, 1) AS trip_duration,        
	    COALESCE(ctd.passenger_count, 1) AS passenger_count, 
	    COALESCE(ctd.trip_src_id, 'n.a.') AS trip_src_id, 
	    COALESCE(ctd.source_system, 'unknown') AS source_system, 
	    COALESCE(ctd.source_entity, 'unknown') AS source_entity,
	    COALESCE(c.start_dt, current_timestamp) AS customer_start_dt, 
	    current_timestamp AS update_dt, 
	    current_timestamp AS insert_dt
	FROM bl_cl.cleaned_taxi_data ctd
	LEFT JOIN vendor_ids v ON ctd.vendor_src_id = v.vendor_src_id
	LEFT JOIN booking_ids b ON ctd.booking_src_id = b.booking_src_id
	LEFT JOIN (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_id, customer_src_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    --WHERE is_active = true
	) c ON ctd.customer_src_id = c.customer_src_id
	LEFT JOIN promo_ids p ON ctd.promo_src_id = p.promo_src_id
	LEFT JOIN payment_ids py ON ctd.payment_src_id = py.payment_src_id
	LEFT JOIN rate_ids r ON ctd.rate_src_id = r.rate_src_id
	LEFT JOIN pickup_locations pl 
	    ON ctd.pickup_longitude IS NOT NULL 
	    AND ctd.pickup_latitude IS NOT NULL
	    AND ctd.pickup_longitude = pl.longitude 
	    AND ctd.pickup_latitude = pl.latitude
	LEFT JOIN dropoff_locations dl 
	    ON ctd.dropoff_longitude IS NOT NULL 
	    AND ctd.dropoff_latitude IS NOT NULL
	    AND ctd.dropoff_longitude = dl.longitude 
	    AND ctd.dropoff_latitude = dl.latitude
	LEFT JOIN bl_3nf.ce_taxi_trips t
	    ON ctd.trip_src_id = t.trip_src_id
	WHERE t.trip_src_id is null 
	    AND (ctd.pickup_datetime < ctd.dropoff_datetime 
	       OR ctd.pickup_datetime IS NULL 
	       OR ctd.dropoff_datetime IS NULL)
	    AND ctd.passenger_count > 0
 		and ctd.pickup_datetime BETWEEN ('2016-01-01 00:00:00'::timestamp - interval '2 months')
              AND '2016-01-01 00:30:00'::timestamp;
	END IF;

----------------------------------------------------------------------------
    -- 2. INCREMENTAL LOAD: update changes & insert new rows
    IF UPPER(load_type) = 'INCREMENTAL' THEN
        RAISE NOTICE 'Incremental load started at %', clock_timestamp();

 
        --2.1 UPDATE existing rows where some fields changed
WITH -- I choose columns that I need it is faster way to fill table
	vendor_ids AS (
	    SELECT DISTINCT ON (vendor_src_id, vendor_telephone) vendor_src_id, vendor_id
	    FROM bl_3nf.ce_vendors
	),
	booking_ids AS (
	    SELECT DISTINCT ON (booking_src_id) booking_src_id, booking_id
	    FROM bl_3nf.ce_bookings
	),
	customer_ids AS (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_src_id, customer_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    WHERE is_active = true
	),
	promo_ids AS (
	    SELECT DISTINCT ON (promo_src_id) promo_src_id, promo_id
	    FROM bl_3nf.ce_promotions
	),
	payment_ids AS (
	    SELECT DISTINCT ON (payment_src_id) payment_src_id, payment_id
	    FROM bl_3nf.ce_payments
	),
	rate_ids AS (
	    SELECT DISTINCT ON (rate_src_id) rate_src_id, rate_id
	    FROM bl_3nf.ce_rates
	),
	pickup_locations AS (
	    SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	),
	dropoff_locations AS (
	    SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	)
	UPDATE bl_3nf.ce_taxi_trips_partition ctt
    SET
        vendor_id = COALESCE(v.vendor_id, -1),
        booking_id = COALESCE(b.booking_id, -1),
        customer_id = COALESCE(c.customer_id, -1),
        promo_id = COALESCE(p.promo_id, -1),
        payment_id = COALESCE(py.payment_id, -1),
        rate_id = COALESCE(r.rate_id, -1),
        pickup_location_id = COALESCE(pl.location_id, -1),
        dropoff_location_id = COALESCE(dl.location_id, -1),
        pickup_datetime = ctd.pickup_datetime,
        dropoff_datetime = ctd.dropoff_datetime,
        distance_miles = COALESCE(ctd.distance_miles, 0.00),
        trip_duration = COALESCE(ctd.trip_duration, 0),
        passenger_count = COALESCE(ctd.passenger_count, 0),
        source_system = COALESCE(NULLIF(ctd.source_system, ''), 'unknown'),
        source_entity = COALESCE(NULLIF(ctd.source_entity, ''), 'unknown'),
        customer_start_dt = c.start_dt,
        update_dt = now()
    FROM bl_cl.cleaned_taxi_data ctd
	LEFT JOIN vendor_ids v ON ctd.vendor_src_id = v.vendor_src_id
	LEFT JOIN booking_ids b ON ctd.booking_src_id = b.booking_src_id
	LEFT JOIN (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_id, customer_src_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    --WHERE is_active = true
	) c ON ctd.customer_src_id = c.customer_src_id
	LEFT JOIN promo_ids p ON ctd.promo_src_id = p.promo_src_id
	LEFT JOIN payment_ids py ON ctd.payment_src_id = py.payment_src_id
	LEFT JOIN rate_ids r ON ctd.rate_src_id = r.rate_src_id
	LEFT JOIN pickup_locations pl 
	    ON ctd.pickup_longitude IS NOT NULL 
	    AND ctd.pickup_latitude IS NOT NULL
	    AND ctd.pickup_longitude = pl.longitude 
	    AND ctd.pickup_latitude = pl.latitude
	LEFT JOIN dropoff_locations dl 
	    ON ctd.dropoff_longitude IS NOT NULL 
	    AND ctd.dropoff_latitude IS NOT NULL
	    AND ctd.dropoff_longitude = dl.longitude 
	    AND ctd.dropoff_latitude = dl.latitude
	--LEFT JOIN bl_3nf.ce_taxi_trips t
	    --ON ctd.trip_src_id = t.trip_src_id
	WHERE 
		ctt.trip_src_id = ctd.trip_src_id
    	AND (
	        --ctt.vendor_id            IS DISTINCT FROM v.vendor_id
	        --OR ctt.booking_id        IS DISTINCT FROM b.booking_id
	        --OR ctt.customer_id       IS DISTINCT FROM c.customer_id
	        --OR ctt.promo_id          IS DISTINCT FROM p.promo_id
	        ---OR ctt.payment_id        IS DISTINCT FROM py.payment_id
	        ---OR ctt.rate_id           IS DISTINCT FROM r.rate_id
	        ---ctt.pickup_location_id  IS DISTINCT FROM pl.location_id
	        ---OR ctt.dropoff_location_id IS DISTINCT FROM dl.location_id
	        ---or ctt.pickup_datetime      IS DISTINCT FROM ctd.pickup_datetime
	        ---or ctt.dropoff_datetime  IS DISTINCT FROM ctd.dropoff_datetime
	        ctt.distance_miles    IS DISTINCT FROM ctd.distance_miles
	        OR ctt.trip_duration     IS DISTINCT FROM ctd.trip_duration
	        OR ctt.passenger_count   IS DISTINCT FROM ctd.passenger_count
	        OR ctt.source_system     IS DISTINCT FROM ctd.source_system
	        OR ctt.source_entity     IS DISTINCT FROM ctd.source_entity
	        --OR ctt.customer_start_dt IS DISTINCT FROM c.start_dt
			and ctd.pickup_datetime BETWEEN ('2016-04-01 00:00:00'::timestamp - interval '2 months')
              AND '2016-04-01 00:00:00'::timestamp
    	);

        --2.2 INSERT new rows if they do not exist in ce_taxi_trips.
WITH -- I choose columns that I need it is faster way to fill table
	vendor_ids AS (
	    SELECT DISTINCT ON (vendor_src_id, vendor_telephone) vendor_src_id, vendor_id
	    FROM bl_3nf.ce_vendors
	),
	booking_ids AS (
	    SELECT DISTINCT ON (booking_src_id) booking_src_id, booking_id
	    FROM bl_3nf.ce_bookings
	),
	customer_ids AS (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_src_id, customer_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    WHERE is_active = true
	),
	promo_ids AS (
	    SELECT DISTINCT ON (promo_src_id) promo_src_id, promo_id
	    FROM bl_3nf.ce_promotions
	),
	payment_ids AS (
	    SELECT DISTINCT ON (payment_src_id) payment_src_id, payment_id
	    FROM bl_3nf.ce_payments
	),
	rate_ids AS (
	    SELECT DISTINCT ON (rate_src_id) rate_src_id, rate_id
	    FROM bl_3nf.ce_rates
	),
	pickup_locations AS (
	    SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	),
	dropoff_locations AS (
	    SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	)
	INSERT INTO bl_3nf.ce_taxi_trips_partition (
	    trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
	    pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
	    distance_miles, trip_duration, passenger_count, trip_src_id,
	    source_system, source_entity, customer_start_dt, update_dt, insert_dt
	)
	SELECT DISTINCT ON (ctd.trip_src_id)
	    nextval('bl_3nf.seq_ce_taxi_trip_partition') AS trip_id,                                     
	    COALESCE(v.vendor_id, -1) AS vendor_id, 
	    COALESCE(b.booking_id, -1) AS booking_id, 
	    COALESCE(c.customer_id, -1) AS customer_id, 
	    COALESCE(p.promo_id, -1) AS promo_id, 
	    COALESCE(py.payment_id, -1) AS payment_id, 
	    COALESCE(r.rate_id, -1) AS rate_id, 
	    COALESCE(pl.location_id, -1) AS pickup_location_id, 
	    COALESCE(dl.location_id, -1) AS dropoff_location_id, 
	    ctd.pickup_datetime,                                  
	    ctd.dropoff_datetime, 
	    COALESCE(ctd.distance_miles, 0.00) AS distance_miles, 
	    COALESCE(ctd.trip_duration, 1) AS trip_duration,        
	    COALESCE(ctd.passenger_count, 1) AS passenger_count, 
	    COALESCE(ctd.trip_src_id, 'n.a.') AS trip_src_id, 
	    COALESCE(ctd.source_system, 'unknown') AS source_system, 
	    COALESCE(ctd.source_entity, 'unknown') AS source_entity,
	    COALESCE(c.start_dt, current_timestamp) AS customer_start_dt, 
	    current_timestamp AS update_dt, 
	    current_timestamp AS insert_dt
	FROM bl_cl.cleaned_taxi_data ctd
	LEFT JOIN vendor_ids v ON ctd.vendor_src_id = v.vendor_src_id
	LEFT JOIN booking_ids b ON ctd.booking_src_id = b.booking_src_id
	LEFT JOIN (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_id, customer_src_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    --WHERE is_active = true
	) c ON ctd.customer_src_id = c.customer_src_id
	LEFT JOIN promo_ids p ON ctd.promo_src_id = p.promo_src_id
	LEFT JOIN payment_ids py ON ctd.payment_src_id = py.payment_src_id
	LEFT JOIN rate_ids r ON ctd.rate_src_id = r.rate_src_id
	LEFT JOIN pickup_locations pl 
	    ON ctd.pickup_longitude IS NOT NULL 
	    AND ctd.pickup_latitude IS NOT NULL
	    AND ctd.pickup_longitude = pl.longitude 
	    AND ctd.pickup_latitude = pl.latitude
	LEFT JOIN dropoff_locations dl 
	    ON ctd.dropoff_longitude IS NOT NULL 
	    AND ctd.dropoff_latitude IS NOT NULL
	    AND ctd.dropoff_longitude = dl.longitude 
	    AND ctd.dropoff_latitude = dl.latitude
	LEFT JOIN bl_3nf.ce_taxi_trips_partition t
	    ON ctd.trip_src_id = t.trip_src_id
	WHERE t.trip_src_id is null 
	    AND (ctd.pickup_datetime < ctd.dropoff_datetime 
	       OR ctd.pickup_datetime IS NULL 
	       OR ctd.dropoff_datetime IS NULL)
	    AND ctd.passenger_count > 0
 		and ctd.pickup_datetime BETWEEN ('2016-04-01 00:00:00'::timestamp - interval '2 months')
              AND '2016-04-01 00:00:00'::timestamp;

        RAISE NOTICE '-- % -- Incremental load completed.', clock_timestamp();
    END IF;




    ----------------------------------------------------------------
    -- Completion log
    RAISE NOTICE 'Load completed successfully at %. Total time: % seconds.',
        clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during % load at %: %', load_type, clock_timestamp(), SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;




--testing
select bl_3nf.load_ce_taxi_trips_partition('full');
select bl_3nf.load_ce_taxi_trips_partition('incremental');


select * from bl_3nf.ce_taxi_trips_partition;
select count(*) from bl_3nf.ce_taxi_trips_2016_01;



-- quantity of unique trip_src_id in source       ------321 rows 
SELECT COUNT(DISTINCT trip_src_id) AS source_keys
FROM bl_3nf.ce_taxi_trips_partition;

-- quantity unique trip_src_id in fact table DM   ----- 995 rows
SELECT COUNT(DISTINCT trip_src_id) AS dm_keys
FROM bl_3nf.ce_taxi_trips;




truncate table bl_3nf.ce_taxi_trips_partition;
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

-- create parental table with partition (this table dont hold data, this table is only logical point for child table data ... only children tables hold data)
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
    distance_miles        DECIMAL(10,2)  NOT NULL CHECK (distance_miles >= 0),
    trip_duration         INT            NOT NULL,
    passenger_count       INT            NOT NULL,
    trip_src_id           VARCHAR(50)   NOT NULL,
    source_system         VARCHAR(255)   NOT NULL,
    source_entity         VARCHAR(255)   NOT NULL,
    customer_start_dt     TIMESTAMP      NOT NULL,
    update_dt             TIMESTAMP      NOT NULL,
    insert_dt             TIMESTAMP      NOT NULL,
    CONSTRAINT chk_ce_taxi_trips_partition_time CHECK (dropoff_datetime > pickup_datetime)
) PARTITION BY RANGE (pickup_datetime);

drop table bl_3nf.ce_taxi_trips_partition;

-- Создание партиции
CREATE TABLE IF NOT EXISTS bl_3nf.ce_taxi_trips_2016_02_01_to_2016_02_05 (
    trip_id               BIGINT         NOT NULL,  
    vendor_id             BIGINT         NOT NULL,
    booking_id            BIGINT         NULL,
    customer_id           BIGINT         NOT NULL,
    promo_id              BIGINT         NULL,
    payment_id            BIGINT         NOT NULL,
    rate_id               BIGINT         NOT NULL,
    pickup_location_id    BIGINT         NOT NULL,
    dropoff_location_id   BIGINT         NOT NULL,
    pickup_datetime       TIMESTAMP      NOT NULL,
    dropoff_datetime      TIMESTAMP      NULL,
    distance_miles        DECIMAL(10,2)  NOT NULL CHECK (distance_miles >= 0),
    trip_duration         INT            NOT NULL,
    passenger_count       INT            NOT NULL,
    trip_src_id           VARCHAR(50)   NOT NULL,
    source_system         VARCHAR(255)   NOT NULL,
    source_entity         VARCHAR(255)   NOT NULL,
    customer_start_dt     TIMESTAMP      NOT NULL,
    update_dt             TIMESTAMP      NOT NULL,
    insert_dt             TIMESTAMP      NOT NULL,
    CONSTRAINT chk_ce_taxi_trips_partition_time CHECK (dropoff_datetime > pickup_datetime)
);

-- Прикрепление партиции к родительской таблице
ALTER TABLE bl_3nf.ce_taxi_trips_partition 
    ATTACH PARTITION bl_3nf.ce_taxi_trips_2016_02_01_to_2016_02_05
    FOR VALUES FROM ('2016-02-01 00:00:00') TO ('2016-02-06 00:00:00');




































































