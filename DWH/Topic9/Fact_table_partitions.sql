
--create parental table (for partitioning)
CREATE TABLE IF NOT EXISTS bl_dm.fct_taxi_trips_partition (
    trip_sur_id          BIGINT ,  
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
    trip_src_id          BIGINT  not null,
    trip_id              VARCHAR(255)  not null,
    trip_duration        INT           not null, 
    passenger_count      INT           not null, 
    distance_miles       DECIMAL(10,2) not null, 
    trip_amount          DECIMAL(10,2) not null,            
    update_dt            TIMESTAMP     not null, 
    insert_dt            TIMESTAMP     not null,
    pickup_datetime      timestamp    
) partition by range(pickup_datetime);

--create complex primary key
ALTER TABLE bl_dm.fct_taxi_trips_partition
    ADD CONSTRAINT pk_fct_taxi_trips_partition PRIMARY KEY (trip_sur_id, pickup_datetime);


--create new partition
ALTER TABLE bl_dm.fct_taxi_trips_partition 
    ATTACH PARTITION bl_dm.fct_taxi_trips_2016_01_03 
    FOR VALUES FROM ('2016-01-01') TO ('2016-03-31');


commit;






































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

      	WHERE ctt.pickup_datetime BETWEEN (''2016-04-01 00:00:00''::timestamp - interval ''3 months'') AND ''2016-04-01 00:00:00''::timestamp
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


CALL bl_dm.load_fct_taxi_trips();

SELECT * FROM bl_dm.fct_taxi_trips;

truncate table bl_dm.fct_taxi_trips;


--Testing

--query for looking how many rows are affected
SELECT * FROM bl_dm.load_logs WHERE procedure_name = 'load_fct_taxi_trips';

-- quantity of unique trip_src_id in source       ------995 rows 
SELECT COUNT(DISTINCT trip_src_id) AS source_keys
FROM bl_3nf.ce_taxi_trips;

-- quantity unique trip_src_id in fact table DM   ----- 481 rows
SELECT COUNT(DISTINCT trip_src_id) AS dm_keys
FROM bl_dm.fct_taxi_trips;












