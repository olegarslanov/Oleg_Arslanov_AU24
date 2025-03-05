-- create temporary tables
CREATE TEMP TABLE tmp_vendor_ids AS
SELECT DISTINCT ON (vendor_src_id, vendor_telephone) vendor_src_id, vendor_id
FROM bl_3nf.ce_vendors
ORDER BY vendor_src_id, vendor_telephone, vendor_id DESC;

CREATE TEMP TABLE tmp_booking_ids AS
SELECT DISTINCT ON (booking_src_id) booking_src_id, booking_id
FROM bl_3nf.ce_bookings
ORDER BY booking_src_id, booking_id DESC;

CREATE TEMP TABLE tmp_customer_ids AS
SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_src_id, customer_id, start_dt
FROM bl_3nf.ce_customers_scd
WHERE is_active = true
ORDER BY customer_src_id, customer_telephone, start_dt DESC;

CREATE TEMP TABLE tmp_promo_ids AS
SELECT DISTINCT ON (promo_src_id) promo_src_id, promo_id
FROM bl_3nf.ce_promotions
ORDER BY promo_src_id, promo_id DESC;

CREATE TEMP TABLE tmp_payment_ids AS
SELECT DISTINCT ON (payment_src_id) payment_src_id, payment_id
FROM bl_3nf.ce_payments
ORDER BY payment_src_id, payment_id DESC;

CREATE TEMP TABLE tmp_rate_ids AS
SELECT DISTINCT ON (rate_src_id) rate_src_id, rate_id
FROM bl_3nf.ce_rates
ORDER BY rate_src_id, rate_id DESC;

CREATE TEMP TABLE tmp_locations AS
SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
FROM bl_3nf.ce_locations
ORDER BY longitude, latitude, location_id DESC;



-- 2016 01
CREATE OR REPLACE PROCEDURE bl_3nf.load_preFact_batches()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start TIMESTAMP := '2016-02-01 00:00:00';
    v_end   TIMESTAMP := '2016-01-31 23:59:59';
    v_batch_interval INTERVAL := '30 minutes';
    v_batch_end TIMESTAMP;
BEGIN
    WHILE v_start < v_end LOOP
        v_batch_end := LEAST(v_start + v_batch_interval, v_end);
        RAISE NOTICE 'Loading batch from % to %', v_start, v_batch_end;

        -- Data insert for one batch
        INSERT INTO bl_3nf.ce_taxi_trips_partition (
            trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
            pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
            distance_miles, trip_duration, passenger_count, trip_src_id,
            source_system, source_entity, customer_start_dt, update_dt, insert_dt
        )
        SELECT 
            nextval('bl_3nf.seq_ce_taxi_trip') AS trip_id,                                     
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
        FROM bl_cl.cleaned_taxi_data_2016_01 ctd
        LEFT JOIN tmp_vendor_ids v ON ctd.vendor_src_id = v.vendor_src_id
        LEFT JOIN tmp_booking_ids b ON ctd.booking_src_id = b.booking_src_id
        LEFT JOIN tmp_customer_ids c ON ctd.customer_src_id = c.customer_src_id
        LEFT JOIN tmp_promo_ids p ON ctd.promo_src_id = p.promo_src_id
        LEFT JOIN tmp_payment_ids py ON ctd.payment_src_id = py.payment_src_id
        LEFT JOIN tmp_rate_ids r ON ctd.rate_src_id = r.rate_src_id
        LEFT JOIN tmp_locations pl 
            ON ctd.pickup_longitude IS NOT NULL 
            AND ctd.pickup_latitude IS NOT NULL
            AND ctd.pickup_longitude = pl.longitude 
            AND ctd.pickup_latitude = pl.latitude
        LEFT JOIN tmp_locations dl 
            ON ctd.dropoff_longitude IS NOT NULL 
            AND ctd.dropoff_latitude IS NOT NULL
            AND ctd.dropoff_longitude = dl.longitude 
            AND ctd.dropoff_latitude = dl.latitude
        WHERE ctd.pickup_datetime BETWEEN v_start AND v_batch_end
          AND (ctd.pickup_datetime < ctd.dropoff_datetime 
               OR ctd.pickup_datetime IS NULL 
               OR ctd.dropoff_datetime IS NULL)
          AND ctd.passenger_count > 0;
			on conflict do nothing;


        COMMIT;

        -- go to the next interval
        v_start := v_batch_end;
    END LOOP;
END;
$$;



-- load data in ce_taxi_trips partition
call bl_3nf.load_preFact_batches();



--Testing

select count(*) from bl_3nf.ce_taxi_trips_partition;
select count(*) from bl_3nf.ce_taxi_trips_2016_01;


SELECT * FROM bl_3nf.ce_taxi_trips_partition;
SELECT * FROM bl_3nf.ce_taxi_trips_2016_01;



