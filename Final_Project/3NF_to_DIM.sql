--create schema
--create schema if not exists bl_dm;

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_ce_load_logs;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_run_id;

--create table load_logs
CREATE table if not exists bl_dm.load_logs (
    log_id BIGINT DEFAULT nextval('bl_dm.seq_ce_load_logs') PRIMARY KEY,
    run_id bigint,
    procedure_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    rows_affected INT,
    status VARCHAR(20) NOT NULL,
    error_message TEXT
);

--drop table bl_dm.load_logs;

--COMMENT ON TABLE bl_dm.load_logs IS 'Логи выполнения процедур загрузки измерений.';
--COMMENT ON COLUMN bl_dm.load_logs.log_id IS 'Уникальный идентификатор лога.';
--COMMENT ON COLUMN bl_dm.load_logs.procedure_name IS 'Название запущенной процедуры.';
--COMMENT ON COLUMN bl_dm.load_logs.start_time IS 'Время начала выполнения.';
--COMMENT ON COLUMN bl_dm.load_logs.end_time IS 'Время окончания выполнения.';
--COMMENT ON COLUMN bl_dm.load_logs.rows_affected IS 'Количество обработанных строк.';
--COMMENT ON COLUMN bl_dm.load_logs.status IS 'Статус выполнения: STARTED, SUCCESS, FAILED.';
--COMMENT ON COLUMN bl_dm.load_logs.error_message IS 'Сообщение об ошибке, если она возникла.';



-------------------------------------------------------------------------------------------------------
--dim_vendors

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_vendors;

--create procedure for load to DIM_vendors from ce_vendors
CREATE OR REPLACE PROCEDURE bl_dm.load_dim_vendors()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := now();
    v_rows_affected INT;
    v_run_id BIGINT := nextval('bl_dm.seq_run_id');
BEGIN
    -- Record the start of the procedure in the log
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_dim_vendors', v_start_time, 'STARTED');

    -- Bulk insert with upsert (merge)
    EXECUTE format(
        'INSERT INTO bl_dm.dim_vendors (
            vendor_sur_id, vendor_name, vendor_street, vendor_house, vendor_city,
            vendor_country, vendor_postal_code, vendor_telephone, vendor_src_id,
            vendor_address_src_id, source_system, source_entity, update_dt, insert_dt
        )
        SELECT distinct
            nextval(''bl_dm.seq_dim_vendors''),
            cv.vendor_name,
            cva.vendor_street,
            cva.vendor_house,
            cva.vendor_city,
            cva.vendor_country,
            cva.vendor_postal_code,
            cv.vendor_telephone,
            cv.vendor_id,
            cv.vendor_address_id,
            cv.source_system,
            cv.source_entity,
            now(),
            now()
        FROM bl_3nf.ce_vendors cv
        LEFT JOIN bl_3nf.ce_vendor_addresses cva
            ON cv.vendor_address_id = cva.vendor_address_id
        ON CONFLICT (vendor_src_id)
        DO UPDATE SET
            vendor_name = EXCLUDED.vendor_name,
            vendor_street = EXCLUDED.vendor_street,
            vendor_house = EXCLUDED.vendor_house,
            vendor_city = EXCLUDED.vendor_city,
            vendor_country = EXCLUDED.vendor_country,
            vendor_postal_code = EXCLUDED.vendor_postal_code,
            vendor_telephone = EXCLUDED.vendor_telephone,
            vendor_src_id = EXCLUDED.vendor_src_id,
            vendor_address_src_id = EXCLUDED.vendor_address_src_id,
            source_system = EXCLUDED.source_system,
            source_entity = EXCLUDED.source_entity,
            update_dt = CASE
                WHEN 
                    EXCLUDED.vendor_name IS DISTINCT FROM dim_vendors.vendor_name OR
                    EXCLUDED.vendor_street IS DISTINCT FROM dim_vendors.vendor_street OR
                    EXCLUDED.vendor_house IS DISTINCT FROM dim_vendors.vendor_house OR
                    EXCLUDED.vendor_city IS DISTINCT FROM dim_vendors.vendor_city OR
                    EXCLUDED.vendor_country IS DISTINCT FROM dim_vendors.vendor_country OR
                    EXCLUDED.vendor_postal_code IS DISTINCT FROM dim_vendors.vendor_postal_code OR
                    EXCLUDED.vendor_telephone IS DISTINCT FROM dim_vendors.vendor_telephone OR
                    EXCLUDED.vendor_src_id IS DISTINCT FROM dim_vendors.vendor_src_id OR
                    EXCLUDED.vendor_address_src_id IS DISTINCT FROM dim_vendors.vendor_address_src_id OR
                    EXCLUDED.source_system IS DISTINCT FROM dim_vendors.source_system OR
                    EXCLUDED.source_entity IS DISTINCT FROM dim_vendors.source_entity
                THEN now()
                ELSE dim_vendors.update_dt
            END;'
    );

    -- Get the number of affected rows
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Record the end of the procedure in the log
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_dim_vendors', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- Completion message
    RAISE NOTICE 'Procedure load_dim_vendors completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error if something goes wrong
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_dim_vendors', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;



--------------------------------------------------------------------------------------------------------------
-- dim_customers_scd

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_customers_scd;

--create unique index;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customers_scd_unique2
ON bl_dm.dim_customers_scd(customer_src_id, start_dt);

--DROP INDEX IF EXISTS idx_dim_customers_scd_unique cascade;


--create procedure for customers_scd
CREATE OR REPLACE PROCEDURE bl_dm.load_dim_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := now();
    v_rows_affected INT;
	v_run_id BIGINT := nextval('bl_dm.seq_run_id');

BEGIN
    -- Record the start of the procedure in the log
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_dim_customers_scd', v_start_time, 'STARTED');
    
 EXECUTE '
    -- A) insert new/changed rows
    INSERT INTO bl_dm.dim_customers_scd (
        customer_sur_id, customer_type, customer_telephone, customer_src_id,
        source_system, source_entity, start_dt, end_dt, is_active, insert_dt
    )
    SELECT
        nextval(''bl_dm.seq_dim_customers_scd''),
        s.customer_type,
        s.customer_telephone,
        s.customer_id,                  
        s.source_system,
        s.source_entity,
        s.start_dt,
        s.end_dt,
        s.is_active,
        s.insert_dt
    FROM (
        SELECT DISTINCT ccs.*
        FROM bl_3nf.ce_customers_scd ccs
        LEFT JOIN bl_dm.dim_customers_scd dcs
            ON ccs.customer_id = dcs.customer_src_id
        WHERE dcs.customer_src_id IS NULL
    ) AS s;

    -- B) active rows close
    UPDATE bl_dm.dim_customers_scd dcs
    SET 
        end_dt = now(), 
        is_active = false
    FROM bl_3nf.ce_customers_scd ccs
    WHERE 
        dcs.customer_src_id = ccs.customer_id
        AND dcs.customer_type = ccs.customer_type
        AND dcs.customer_telephone = ccs.customer_telephone
        AND dcs.source_system = ccs.source_system
        AND dcs.source_entity = ccs.source_entity
        AND ccs.is_active = false
        AND dcs.is_active = true
        AND dcs.end_dt = ''9999-12-31 23:59:59''::timestamp;
';


    -- Get the number of affected rows
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Record the end of the procedure in the log
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_dim_customers_scd', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- Completion message
    RAISE NOTICE 'Procedure load_dim_customers_scd completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error if something goes wrong
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_dim_customers_scd', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;

CALL bl_dm.load_dim_customers_scd();

---------------------------------------------------------------------------------------------------------------
--dim_rates

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_rates;

--unique index (we need for on conflict that interpretaor knows that is must be unique or something like that)
--CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_rate_src_id
--ON bl_dm.dim_rates (rate_src_id);

--DROP INDEX IF EXISTS idx_unique_rate_src_id cascade;

--create procedure
CREATE OR REPLACE PROCEDURE bl_dm.load_dim_rates()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := now();
    v_rows_affected INT;
	v_run_id BIGINT := nextval('bl_dm.seq_run_id');
BEGIN
    -- Log the start of the procedure
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_dim_rates', v_start_time, 'STARTED');

    -- Bulk insert with upsert (merge)
    EXECUTE format(
	    'INSERT INTO bl_dm.dim_rates (
	    rate_sur_id, base_fare, rate_per_mile, rate_src_id,
	    source_system, source_entity, update_dt, insert_dt
		)
		SELECT distinct
		    nextval(''bl_dm.seq_dim_rates''),
		    c.base_fare,
 			c.rate_per_mile,
 			c.rate_id,                                        
		    c.source_system,
 			c.source_entity,
 			now(),
 			now()
		FROM bl_3nf.ce_rates c
		ON CONFLICT (rate_src_id)
		DO UPDATE SET
		    base_fare = EXCLUDED.base_fare,
		    rate_per_mile = EXCLUDED.rate_per_mile,
		    source_system = EXCLUDED.source_system,
		    source_entity = EXCLUDED.source_entity,
		    update_dt = CASE
                WHEN 
                    EXCLUDED.base_fare IS DISTINCT FROM dim_rates.base_fare OR
                    EXCLUDED.rate_per_mile IS DISTINCT FROM dim_rates.rate_per_mile OR
                    EXCLUDED.source_system IS DISTINCT FROM dim_rates.source_system OR
                    EXCLUDED.source_entity IS DISTINCT FROM dim_rates.source_entity
                THEN now()
                ELSE dim_rates.update_dt
            END;'
	);

    -- Get the number of affected rows
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Log the successful completion of the procedure
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_dim_rates', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- Completion message
    RAISE NOTICE 'Procedure load_dim_rates completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error if something goes wrong
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_dim_rates', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




---------------------------------------------------------------------------------------------------------------------
--dim_promotions

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_promotions;

-- Create unique index for ON CONFLICT resolution
--CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_promotions_promo_src_id
--ON bl_dm.dim_promotions (promo_src_id);

--DROP INDEX IF EXISTS idx_dim_promotions_promo_src_id cascade;

-- Create procedure for loading DIM_PROMOTIONS
CREATE OR REPLACE PROCEDURE bl_dm.load_dim_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := now();
    v_rows_affected INT;
	v_run_id BIGINT := nextval('bl_dm.seq_run_id');
BEGIN
    -- Log the start of the procedure
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_dim_promotions', v_start_time, 'STARTED');

    -- Bulk insert with upsert (merge)
    EXECUTE format(
    'INSERT INTO bl_dm.dim_promotions (
        promo_sur_id, promo_code, discount_percentage, promo_src_id,
        source_system, source_entity, update_dt, insert_dt
    )
    SELECT distinct
        nextval(''bl_dm.seq_dim_promotions''),
        promo_code,
 		discount_percentage,
 		promo_id,                         
        source_system,
 		source_entity,
 		now(),
 		insert_dt
    FROM bl_3nf.ce_promotions
    ON CONFLICT (promo_src_id)
    DO UPDATE SET
        promo_code = EXCLUDED.promo_code,
        discount_percentage = EXCLUDED.discount_percentage,
        source_system = EXCLUDED.source_system,
        source_entity = EXCLUDED.source_entity,
  		update_dt = CASE
                WHEN 
                    EXCLUDED.promo_code IS DISTINCT FROM dim_promotions.promo_code OR
                    EXCLUDED.discount_percentage IS DISTINCT FROM dim_promotions.discount_percentage OR
                    EXCLUDED.source_system IS DISTINCT FROM dim_promotions.source_system OR
                    EXCLUDED.source_entity IS DISTINCT FROM dim_promotions.source_entity
                THEN now()
                ELSE dim_promotions.update_dt
            END;'
    );

    -- Get the number of affected rows
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Log the successful completion of the procedure
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_dim_promotions', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- Completion message
    RAISE NOTICE 'Procedure load_dim_promotions completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error if something goes wrong
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_dim_promotions', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




--------------------------------------------------------------------------------------------------------------------------
-- dim_dates

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_dates;

--fill dates table
INSERT INTO bl_dm.dim_dates (
    date_sur_id, calendar_date, day_of_week, day, month, quarter, year
)
SELECT 
    nextval('bl_dm.seq_dim_dates'),
    date_series AS calendar_date,
    to_char(date_series, 'Day') AS day_of_week,
    extract(day FROM date_series) AS day,
    extract(month FROM date_series) AS month,
    extract(quarter FROM date_series) AS quarter,
    extract(year FROM date_series) AS year
FROM generate_series(
    '2010-01-01'::date, 
    '2030-12-31'::date, 
    '1 day'::interval
) AS date_series
ON CONFLICT (calendar_date) DO NOTHING;  -- Avoid duplicates



----------------------------------------------------------------------------------------------------------------------------
--dim_time

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_time;

--fill time table
INSERT INTO bl_dm.dim_time (
    time_sur_id, calendar_time, hour, minute
)
SELECT 
    nextval('bl_dm.seq_dim_time'),
    (time_series)::TIME AS calendar_time,
    extract(hour FROM time_series) AS hour,
    extract(minute FROM time_series) AS minute
FROM generate_series(
    '2000-01-01 00:00:00'::timestamp,  -- use fictive date for generate all posible hour and minutes
    '2000-01-01 23:59:00'::timestamp,  
    '1 minute'::interval
) AS time_series
ON CONFLICT (calendar_time) DO NOTHING;  -- avoid duplicates



----------------------------------------------------------------------------
-- dim_locations

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_locations;

--create unique index;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_locations_unique
ON bl_dm.dim_locations(location_src_id, latitude, longitude);

--DROP INDEX IF EXISTS idx_dim_locations_unique cascade;

SELECT conname, contype, pg_get_constraintdef(oid) 
FROM pg_constraint 
WHERE conrelid = 'bl_dm.dim_locations'::regclass;


-- Create procedure for loading dim_locations
CREATE OR REPLACE PROCEDURE bl_dm.load_dim_locations()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := now();
    v_rows_affected INT;
	v_run_id BIGINT := nextval('bl_dm.seq_run_id');
BEGIN
    -- Log the start of the procedure
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_dim_locations', v_start_time, 'STARTED');

    -- Insert new records and update changed records
    EXECUTE '
        INSERT INTO bl_dm.dim_locations (
            location_sur_id, longitude, latitude, location_src_id,
            source_system, source_entity, update_dt, insert_dt
        )
        SELECT distinct
            nextval(''bl_dm.seq_dim_locations''),
            longitude,
 			latitude,
 			location_id,                         
            source_system,
 			source_entity,
 			now(),
 			now()
        FROM 
	        bl_3nf.ce_locations	
        ON CONFLICT (location_src_id, latitude, longitude) 
        DO UPDATE SET
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            source_system = EXCLUDED.source_system,
            source_entity = EXCLUDED.source_entity,
            update_dt = CASE 
                WHEN dim_locations.longitude IS DISTINCT FROM EXCLUDED.longitude
                  OR dim_locations.latitude IS DISTINCT FROM EXCLUDED.latitude
                  OR dim_locations.source_system IS DISTINCT FROM EXCLUDED.source_system
                  OR dim_locations.source_entity IS DISTINCT FROM EXCLUDED.source_entity
                THEN now()
                ELSE dim_locations.update_dt
            END;';

    -- Get the number of affected rows
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Log the successful completion of the procedure
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_dim_locations', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- Completion message
    RAISE NOTICE 'Procedure load_dim_locations completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error if something goes wrong
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_dim_locations', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'dim_locations';
SELECT conname, pg_get_constraintdef(oid) 
FROM pg_constraint 
WHERE conrelid = 'bl_dm.dim_locations'::regclass;


----------------------------------------------------------------------------------------------------------------------------
--dim_junk_attributes

-- Create sequence for surrogate keys
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_junk_attributes;

-- Create unique index for conflict resolution
--CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_payment_booking_src_id 
--ON bl_dm.dim_junk_attributes (payment_src_id, booking_src_id);

--DROP INDEX IF EXISTS idx_unique_payment_src_id;


-- Create procedure for loading data into DIM_JUNK_ATTRIBUTES
CREATE OR REPLACE PROCEDURE bl_dm.load_dim_junk_attributes()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := now();
    v_rows_affected INT;
	v_run_id BIGINT := nextval('bl_dm.seq_run_id');
BEGIN
    -- Log procedure start
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_dim_junk_attributes', v_start_time, 'STARTED');

    -- Insert or update data from 3NF to DIM_JUNK_ATTRIBUTES
    EXECUTE '
    INSERT INTO bl_dm.dim_junk_attributes (
        junk_sur_id, payment_type, booking_type, 
        booking_src_id, payment_src_id, 
        source_system, source_entity, update_dt, insert_dt
    )
    SELECT DISTINCT ON (p.payment_src_id)
        nextval(''bl_dm.seq_dim_junk_attributes''),
        COALESCE(p.payment_type, ''n.a.'') AS payment_type,
        COALESCE(b.booking_type, ''n.a.'') AS booking_type,
        b.booking_id,                                            
        p.payment_id,                                          
        p.source_system, 
        p.source_entity, 
        now(), 
        now()
    FROM bl_3nf.ce_taxi_trips tt
    LEFT JOIN bl_3nf.ce_payments p
        ON tt.payment_id = p.payment_id
    LEFT JOIN bl_3nf.ce_bookings b
        ON tt.booking_id = b.booking_id
    ORDER BY p.payment_src_id, p.update_dt DESC
    ON CONFLICT (payment_src_id) 
    DO UPDATE SET
        payment_type = EXCLUDED.payment_type,
        booking_type = EXCLUDED.booking_type,
        source_system = EXCLUDED.source_system,
        source_entity = EXCLUDED.source_entity,
        update_dt = 
            CASE 
                WHEN dim_junk_attributes.payment_type IS DISTINCT FROM EXCLUDED.payment_type
                  OR dim_junk_attributes.booking_type IS DISTINCT FROM EXCLUDED.booking_type
                  OR dim_junk_attributes.source_system IS DISTINCT FROM EXCLUDED.source_system
                  OR dim_junk_attributes.source_entity IS DISTINCT FROM EXCLUDED.source_entity
                THEN now()
                ELSE dim_junk_attributes.update_dt
            END;';

    
    -- Get affected rows count
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Log procedure success
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_dim_junk_attributes', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- Notify about successful execution
    RAISE NOTICE 'Procedure load_dim_junk_attributes completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- Log error if something goes wrong
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_dim_junk_attributes', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;

-------------------------------------------
SELECT distinct
            nextval('bl_dm.seq_dim_junk_attributes'),
            COALESCE(p.payment_type, 'n.a.') AS payment_type,
            COALESCE(b.booking_type, 'n.a.') AS booking_type,
            b.booking_id,
            p.payment_id,
            p.source_system, 
            p.source_entity, 
            now(), 
            now()
        from bl_3nf.ce_taxi_trips tt
		left join bl_3nf.ce_payments p
			on tt.payment_id = p.payment_id
        LEFT JOIN bl_3nf.ce_bookings b
            on tt.booking_id = b.booking_id;




-----------------------------------------------------------------------------------------------------------------------------
-- fct_taxi_trips

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
	v_run_id BIGINT := nextval('bl_dm.seq_run_id');
BEGIN
    ----------------------------------------------------------------
    -- 1. Start info to log
    ----------------------------------------------------------------
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, status)
    VALUES (v_run_id, 'load_fct_taxi_trips', v_start_time, 'STARTED');

    ----------------------------------------------------------------
    -- 2. Insert
    ----------------------------------------------------------------
   RAISE NOTICE 'Executing INSERT into fct_taxi_trips...';

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
			coalesce(dja.junk_sur_id, -1) as junk_sur_id,   -- i use here coalesce because i dont have values in junk table but i want to add rows :) 
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

		FROM bl_3nf.ce_taxi_trips ctt
		
		-- 1. direct joins
		LEFT JOIN bl_3nf.ce_bookings cby
		    ON ctt.booking_id = cby.booking_id
		LEFT JOIN bl_3nf.ce_payments cpy
		    ON ctt.payment_id = cpy.payment_id

		LEFT JOIN bl_dm.dim_vendors dv 
		    ON dv.vendor_src_id = ctt.vendor_id
		LEFT JOIN bl_dm.dim_customers_scd dcs
		    ON dcs.customer_src_id = ctt.customer_id
		    AND dcs.is_active = TRUE
		LEFT JOIN bl_dm.dim_junk_attributes dja
		    ON dja.payment_src_id = ctt.payment_id
		    AND dja.booking_src_id = ctt.booking_id
		LEFT JOIN bl_dm.dim_rates dr
		    ON dr.rate_src_id = ctt.rate_id
		LEFT JOIN bl_dm.dim_promotions dp
		    ON dp.promo_src_id = ctt.promo_id

		-- 2. join `DIM_LOCATIONS`
		LEFT JOIN bl_dm.dim_locations dlp
		    ON dlp.location_src_id = ctt.pickup_location_id
		LEFT JOIN bl_dm.dim_locations dld
		    ON dld.location_src_id = ctt.dropoff_location_id

		
		-- 3. one `date_sur_id` for all date columns
		LEFT JOIN bl_dm.dim_dates d_common
		    ON d_common.calendar_date = COALESCE(
		        DATE(cby.booking_datetime),  -- firstly booking
		        DATE(ctt.pickup_datetime),   -- then pickup
		        DATE(ctt.dropoff_datetime),  -- then dropoff
		        DATE(cpy.payment_datetime)   -- payment
		    )
		
		-- Use same `date_sur_id`for all columns `FCT_TAXI_TRIPS`
		LEFT JOIN bl_dm.dim_dates ddb
		    ON ddb.date_sur_id = d_common.date_sur_id
		LEFT JOIN bl_dm.dim_dates ddp
		    ON ddp.date_sur_id = d_common.date_sur_id
		LEFT JOIN bl_dm.dim_dates ddd
		    ON ddd.date_sur_id = d_common.date_sur_id
		LEFT JOIN bl_dm.dim_dates ddpy
		    ON ddpy.date_sur_id = d_common.date_sur_id


		-- 4. one `time_sur_id` for all time columns
		LEFT JOIN bl_dm.dim_time t_common
		    ON t_common.calendar_time = COALESCE(
		        CAST(cby.booking_datetime AS TIME),  -- booking
		        CAST(ctt.pickup_datetime AS TIME),   -- pickup
		        CAST(ctt.dropoff_datetime AS TIME),  -- dropoff
		        CAST(cpy.payment_datetime AS TIME)   -- payment
		    )
		
		-- Use same `time_sur_id`** for all columns `FCT_TAXI_TRIPS`
		LEFT JOIN bl_dm.dim_time dtp
		    ON dtp.time_sur_id = t_common.time_sur_id
		LEFT JOIN bl_dm.dim_time dtd
		    ON dtd.time_sur_id = t_common.time_sur_id
		LEFT JOIN bl_dm.dim_time dtb
		    ON dtb.time_sur_id = t_common.time_sur_id
		LEFT JOIN bl_dm.dim_time dtpy
		    ON dtpy.time_sur_id = t_common.time_sur_id

    
        ON CONFLICT (trip_src_id)
        DO UPDATE
        SET
	        trip_duration         = EXCLUDED.trip_duration,
            passenger_count       = EXCLUDED.passenger_count,
			distance_miles        = EXCLUDED.distance_miles,
            trip_amount           = EXCLUDED.trip_amount,
            update_dt = CASE
                WHEN 
					  fct_taxi_trips.distance_miles           IS DISTINCT FROM EXCLUDED.distance_miles
	                  OR fct_taxi_trips.trip_duration         IS DISTINCT FROM EXCLUDED.trip_duration
	                  OR fct_taxi_trips.passenger_count       IS DISTINCT FROM EXCLUDED.passenger_count
                THEN now()
                ELSE fct_taxi_trips.update_dt
            END;
    ';

    ----------------------------------------------------------------
    -- 3. got how many rows are affected
    ----------------------------------------------------------------
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    ----------------------------------------------------------------
    -- 4. End of procedure to log
    ----------------------------------------------------------------
    INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status)
    VALUES (v_run_id, 'load_fct_taxi_trips', v_start_time, now(), v_rows_affected, 'SUCCESS');

    -- end notice
    RAISE NOTICE 'Procedure load_fct_taxi_trips completed. Rows affected: %', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        -- error log
        INSERT INTO bl_dm.load_logs (run_id, procedure_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (v_run_id, 'load_fct_taxi_trips', v_start_time, now(), 0, 'FAILED', SQLERRM);
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


------------------------------------------------------------------------------------------------------------------

-- Execute the procedures
CALL bl_dm.load_dim_vendors();
CALL bl_dm.load_dim_customers_scd();
CALL bl_dm.load_dim_rates();
CALL bl_dm.load_dim_promotions();
CALL bl_dm.load_dim_locations();
CALL bl_dm.load_dim_junk_attributes();
CALL bl_dm.load_fct_taxi_trips();


-- Check inserted data
SELECT * FROM bl_dm.dim_vendors;
SELECT * FROM bl_dm.dim_customers_scd;
SELECT * FROM bl_dm.dim_rates;
SELECT * FROM bl_dm.dim_promotions;
SELECT * FROM bl_dm.dim_locations;
SELECT * FROM bl_dm.dim_junk_attributes;
SELECT * FROM bl_dm.fct_taxi_trips;



SELECT * FROM bl_dm.dim_dates;
SELECT * FROM bl_dm.dim_time;


SELECT count(*) FROM bl_dm.dim_vendors;                  -- 2
SELECT count(*) FROM bl_dm.dim_customers_scd;            -- 382 641
SELECT count(*) FROM bl_dm.dim_rates;                    -- 2
SELECT count(*) FROM bl_dm.dim_promotions;               -- 4
SELECT count(*) FROM bl_dm.dim_locations;                -- 1 752 752
SELECT count(*) FROM bl_dm.dim_junk_attributes;          -- 872 178
SELECT count(*) FROM bl_dm.fct_taxi_trips;               -- 876 376


SELECT *
FROM bl_dm.fct_taxi_trips
WHERE dropoff_date_id IS NULL 
   OR booking_date_id IS NULL 
   OR pickup_time_id IS NULL 
   OR dropoff_time_id IS NULL;

SELECT 
    COUNT(*) FILTER (WHERE dropoff_date_id IS NULL) AS null_dropoff_date,
    COUNT(*) FILTER (WHERE booking_date_id IS NULL) AS null_booking_date,
    COUNT(*) FILTER (WHERE pickup_time_id IS NULL) AS null_pickup_time,
    COUNT(*) FILTER (WHERE dropoff_time_id IS NULL) AS null_dropoff_time
FROM bl_dm.fct_taxi_trips;

SELECT COUNT(DISTINCT dropoff_date_id) AS unique_dropoff_dates
FROM bl_dm.fct_taxi_trips;

SELECT 
    MIN(dropoff_date_id) AS min_dropoff_date,
    MAX(dropoff_date_id) AS max_dropoff_date
FROM bl_dm.fct_taxi_trips;

SELECT DISTINCT f.dropoff_date_id
FROM bl_dm.fct_taxi_trips f
LEFT JOIN bl_dm.dim_dates d ON f.dropoff_date_id = d.date_sur_id
WHERE d.date_sur_id IS NULL;






--check log
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_vendors';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_customers_scd';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_rates';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_promotions';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_dates';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_time';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_locations';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_dim_junk_attributes';
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_fct_taxi_trips';


SELECT pg_size_pretty(pg_database_size('nyc_taxi'));

SELECT pg_switch_wal();
SELECT pg_checkpoint();

vacuum full;


-- truncate
truncate table bl_dm.dim_vendors;
truncate table bl_dm.dim_customers_scd;
truncate table bl_dm.dim_rates;
truncate table bl_dm.dim_promotions;
truncate table bl_dm.dim_locations;
truncate table bl_dm.dim_junk_attributes;
truncate table bl_dm.fct_taxi_trips;



--------------------------------------------------------------------------------------------------------------------------
--my drafts

--check that is no NULL (for PowerBi correct work)
SELECT 
    COUNT(*) AS null_booking_date,
    COUNT(*) FILTER (WHERE booking_date_id IS NULL) AS null_booking_date_count,
    COUNT(*) FILTER (WHERE dropoff_date_id IS NULL) AS null_dropoff_date_count,
    COUNT(*) FILTER (WHERE pickup_time_id IS NULL) AS null_pickup_time_count,
    COUNT(*) FILTER (WHERE dropoff_time_id IS NULL) AS null_dropoff_time_count
FROM bl_dm.fct_taxi_trips;

select count(*) from bl_3nf.ce_promotions;        -- 4             
select count(*) from bl_3nf.ce_customers_scd;     --382 641     
select count(*) from bl_3nf.ce_bookings;          --184 327   
select count(*) from bl_3nf.ce_payments;          --876 376    
select count(*) from bl_3nf.ce_rates;             -- 2        
select count(*) from bl_3nf.ce_locations;         --1 752 752 
select count(*) from bl_3nf.ce_vendor_addresses;  -- 2        
select count(*) from bl_3nf.ce_vendors;           -- 2        
select count(*) from bl_3nf.ce_taxi_trips;        --876 376  

