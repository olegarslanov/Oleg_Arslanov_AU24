--create schema 
CREATE schema if not exists bl_3nf;

--CREATE LOGIC TO LOAD OBJECTS FROM SOURCE TO 3NF LAYER (full/incremental, log, exception)

--I. Users
--look name of current_user
SELECT current_user;

--create priviligies for current user
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_cl TO postgres;
GRANT USAGE ON SCHEMA bl_cl TO postgres;


--II. Load data to 3NF tables


-- default row in each table (without prefact)

-- 1. Promotions
INSERT INTO bl_3nf.ce_promotions 
VALUES (-1, 'n.a.', 0, 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;

-- 2. Bookings
INSERT INTO bl_3nf.ce_bookings 
VALUES (-1, 'n.a.', NULL, 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;

-- 3. Customers
INSERT INTO bl_3nf.ce_customers_scd 
VALUES (-1, 'n.a.', 'n.a.', TRUE, current_timestamp, '9999-12-31 23:59:59', 'n.a.', 'default', 'default', current_timestamp)
ON CONFLICT DO NOTHING;

-- 4. Payments
INSERT INTO bl_3nf.ce_payments 
VALUES (-1, 'n.a.', NULL, 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;

-- 5. Rates
INSERT INTO bl_3nf.ce_rates 
VALUES (-1, 0.00, 0.00, 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;

-- 6. Vendor Addresses
INSERT INTO bl_3nf.ce_vendor_addresses 
VALUES (-1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;

-- 7. Locations
INSERT INTO bl_3nf.ce_locations 
VALUES (-1, 0.00, 0.00, 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;

-- 8. Vendors
INSERT INTO bl_3nf.ce_vendors 
VALUES (-1, -1, 'n.a.', 'n.a.', 'n.a.', 'default', 'default', current_timestamp, current_timestamp)
ON CONFLICT DO NOTHING;



----------------------------------------------------------------------------------------------------------------------
--ce_promotions

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_promotions;

--create indexes
CREATE INDEX if not exists idx_promo_src_id ON bl_3nf.ce_promotions (promo_src_id);
CREATE INDEX if not exists idx_promo_src_id_cleaned ON bl_cl.cleaned_taxi_data (promo_src_id);


-- function
create or replace function bl_3nf.load_ce_promotions (load_type text)
returns void as $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp(); --postgre function for get current date/time
	v_promo_src_id varchar;                  -- for new value in cleaned_taxi_data
    v_promo_code varchar;
    v_discount_percentage integer;
    v_existing_promo_id bigint;            -- for check existing values in ce_promotions
    v_existing_code varchar;
    v_existing_percentage integer;
	v_source_system varchar;
	v_source_entity varchar;

begin
-- log start proccess
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- check value of load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

----------------------------------------------
-- 1. If full load: table cleaning and insert
    IF upper(load_type) = 'FULL' THEN
        RAISE NOTICE '-- % -- Full load started. Truncating table...', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_promotions cascade;
        RAISE NOTICE '-- % -- Full load. Table truncated.', clock_timestamp();
    
     -- insert new row
    RAISE NOTICE '-- % -- Inserting new records ...', clock_timestamp();

	INSERT INTO bl_3nf.ce_promotions (
	    promo_id, 
	    promo_code, 
	    discount_percentage, 
	    promo_src_id, 
	    source_system, 
	    source_entity, 
	    update_dt, 
	    insert_dt
	)
	SELECT distinct on (ctd.promo_src_id)
	    nextval('bl_3nf.seq_ce_promotions'), 
	    COALESCE(NULLIF(ctd.promo_code, ''), 'n.a.'), 
	    COALESCE(ctd.discount_percentage, 0), 
	    COALESCE(NULLIF(ctd.promo_src_id, ''), 'n.a.'),
	    COALESCE(ctd.source_system, 'unknown'), 
	    COALESCE(ctd.source_entity, 'unknown'),
	    current_timestamp, 
	    current_timestamp 
	FROM bl_cl.cleaned_taxi_data ctd
	left join bl_3nf.ce_promotions cp
		on ctd.promo_src_id = cp.promo_src_id
	where cp.promo_src_id is null  --insert only new promo
limit 950000;              

	end if;

----------------------------------------------------
-- 2. Incremental load if new rows added (it it slow but sturdy purpose I use loop)
    IF upper(load_type) = 'INCREMENTAL' THEN
        RAISE NOTICE '-- % -- Incremental load started. Checking for changes...', clock_timestamp();
        
	-- cheking for same rows in cleaned_taxi_data and ce_promotions (if yes saving data to func declared values)
    FOR v_promo_src_id, v_promo_code, v_discount_percentage, v_source_system, v_source_entity IN    -- for every row in cleaned_taxi_data
        SELECT distinct promo_src_id, promo_code, discount_percentage, 
			COALESCE(NULLIF(source_system, ''), 'unknown'), 
            COALESCE(NULLIF(source_entity, ''), 'unknown')
        FROM bl_cl.cleaned_taxi_data ctd
	LOOP                                                                              
        SELECT promo_id, promo_code, discount_percentage        -- here execute code for every row in cleaned_taxi_data (checking for same row in ce_promotions table)
        INTO v_existing_promo_id, v_existing_code, v_existing_percentage
        FROM bl_3nf.ce_promotions
        WHERE promo_src_id = v_promo_src_id
        LIMIT 1;

            -- A. if promotion exists, cheking changes
            IF v_existing_promo_id IS NOT NULL THEN
                
				-- if values changed
                IF v_promo_code IS DISTINCT FROM v_existing_code                      -- comparision uchityvaja NULL 
                OR v_discount_percentage IS DISTINCT FROM v_existing_percentage THEN
                    
                    -- update columns
                    UPDATE bl_3nf.ce_promotions
                    SET 
						update_dt = current_timestamp,
						promo_code = v_promo_code,
						discount_percentage = v_discount_percentage
                    WHERE promo_id = v_existing_promo_id;
                    
                    RAISE NOTICE 'Updated promotion: %', v_promo_src_id;

                ELSE
                    RAISE NOTICE 'No changes for promotion: %', v_promo_src_id;
                END IF;

            -- B. if promotion not exists insert new row
            ELSE
				INSERT INTO bl_3nf.ce_promotions (
				    promo_id, 
				    promo_code, 
				    discount_percentage, 
				    promo_src_id, 
				    source_system, 
				    source_entity, 
				    update_dt, 
				    insert_dt
				)
				values( 
				    nextval('bl_3nf.seq_ce_promotions'), 
				    v_promo_code, 
				    v_discount_percentage, 
				    v_promo_src_id,
				    COALESCE(v_source_system, 'unknown'), 
    				COALESCE(v_source_entity, 'unknown'),
				    current_timestamp, 
				    current_timestamp 
				);
 			RAISE NOTICE 'Inserted new promotion: %', v_promo_src_id;
        	END IF;
        END LOOP;
    END IF;

------------------------------------
    -- log sucsecfull 
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --', clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    -- errors 
    WHEN OTHERS THEN
        RAISE NOTICE '-- % -- Error during % load: %', clock_timestamp(), load_type, SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;



-----------------------------------------------------------------------------------------------------------
--ce_customers_scd

--create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_customers_scd;

-- Create index to optimize queries
CREATE INDEX IF NOT EXISTS idx_ce_customers_scd ON bl_3nf.ce_customers_scd (customer_src_id, customer_type, customer_telephone, is_active);

CREATE INDEX IF NOT EXISTS idx_cust_active ON bl_3nf.ce_customers_scd (customer_src_id, customer_type, customer_telephone, is_active);

--create func
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_customers_scd(load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_mod_time   TIMESTAMP; 
BEGIN
    ----------------------------------------------------------------
    -- Log start of the process
    RAISE NOTICE 'Start of % load at %', load_type, v_start_time;

    -- Validate input parameter
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    ----------------------------------------------------------------
    -- 1. FULL LOAD
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE 'Full load started at %', clock_timestamp();

        -- Truncate the table
        TRUNCATE TABLE bl_3nf.ce_customers_scd;
        RAISE NOTICE 'Table ce_customers_scd truncated at %', clock_timestamp();

             
	INSERT INTO bl_3nf.ce_customers_scd (
	    customer_id,
	    customer_type,
	    customer_telephone,
	    is_active,
	    start_dt,
	    end_dt,
	    customer_src_id,
	    source_system,
	    source_entity,
	    insert_dt
	)
	SELECT distinct
	    nextval('bl_3nf.seq_ce_customers_scd'),
	    sub.customer_type,
	    sub.customer_telephone,
	    true,                                         -- Active by default
	    current_timestamp,                            -- start_dt
	    '9999-12-31 23:59:59'::timestamp,             -- end_dt
	    sub.customer_src_id,
	    sub.source_system,
	    sub.source_entity,
	    current_timestamp                             -- insert_dt
	FROM 
	(
   
      ------------------------------------------------------------------
      -- 1 customer_src_id ≠ 'n.a.'
      ------------------------------------------------------------------
      (SELECT distinct
             customer_src_id,
             COALESCE(NULLIF(customer_type, ''), 'n.a.')       AS customer_type,
             COALESCE(NULLIF(customer_telephone, ''), 'n.a.')  AS customer_telephone,
             COALESCE(source_system, 'unknown')                AS source_system,
             COALESCE(source_entity, 'unknown')                AS source_entity
      FROM bl_cl.cleaned_taxi_data
      WHERE customer_src_id <> 'n.a.') 

      UNION ALL
    
      ------------------------------------------------------------------
      -- 2 customer_src_id = 'n.a.'
      ------------------------------------------------------------------
      (SELECT DISTINCT --ON (customer_src_id)
             customer_src_id,
             COALESCE(NULLIF(customer_type, ''), 'n.a.')       AS customer_type,
             COALESCE(NULLIF(customer_telephone, ''), 'n.a.')  AS customer_telephone,
             COALESCE(source_system, 'unknown')                AS source_system,
             COALESCE(source_entity, 'unknown')                AS source_entity
      FROM bl_cl.cleaned_taxi_data
      WHERE customer_src_id = 'n.a.'
      ORDER BY 
             customer_src_id   -- must match DISTINCT ON

      LIMIT 1)
    ) AS sub

	LEFT JOIN bl_3nf.ce_customers_scd ccs
	       ON sub.customer_src_id = ccs.customer_src_id
	WHERE ccs.customer_src_id IS NULL
limit 950000;

        RAISE NOTICE 'Full load completed at %', clock_timestamp();
    END IF;

    ----------------------------------------------------------------
    -- 2. INCREMENTAL LOAD
    IF UPPER(load_type) = 'INCREMENTAL' THEN
        RAISE NOTICE 'Incremental load started at %', clock_timestamp();

        -- Record modification time
        v_mod_time := clock_timestamp();

        ----------------------------------------------------------------------------
        -- 1) Close active records if they no longer exist in cleaned_taxi_data
        UPDATE bl_3nf.ce_customers_scd ccs
        SET 
            end_dt = v_mod_time,
            is_active = false
        WHERE 
            ccs.is_active = true
            AND NOT EXISTS (
                SELECT 1
                FROM bl_cl.cleaned_taxi_data t
                WHERE t.customer_src_id = ccs.customer_src_id
                  AND t.customer_type = ccs.customer_type
                  AND t.customer_telephone = ccs.customer_telephone
            );

        ----------------------------------------------------------------------------
        -- 2) Insert new active records for (src_id, type, tel) combinations
        --    that do not exist in the SCD (in active state).
        INSERT INTO bl_3nf.ce_customers_scd (
            customer_id,
            customer_type,
            customer_telephone,
            is_active,
            start_dt,
            end_dt,
            customer_src_id,
            source_system,
            source_entity,
            insert_dt
        )
        SELECT
            nextval('bl_3nf.seq_ce_customers_scd'),
            COALESCE(NULLIF(t.customer_type, ''), 'n.a.') AS customer_type,
            COALESCE(NULLIF(t.customer_telephone, ''), 'n.a.') AS customer_telephone,
            true,
            v_mod_time,
            '9999-12-31 23:59:59'::timestamp,
            t.customer_src_id,
            COALESCE(t.source_system,  'unknown') AS source_system,
            COALESCE(t.source_entity, 'unknown') AS source_entity,
            v_mod_time
        FROM bl_cl.cleaned_taxi_data t
        LEFT JOIN bl_3nf.ce_customers_scd ccs
              ON  ccs.customer_src_id = t.customer_src_id
              AND ccs.customer_type = t.customer_type
              AND ccs.customer_telephone = t.customer_telephone
              AND ccs.is_active = true
        WHERE ccs.customer_id IS NULL;  -- No active record exists

		-- 3) Ensure only one record for 'n.a.'
	    INSERT INTO bl_3nf.ce_customers_scd (
	        customer_id,
	        customer_type,
	        customer_telephone,
	        is_active,
	        start_dt,
	        end_dt,
	        customer_src_id,
	        source_system,
	        source_entity,
	        insert_dt
	    )
	    SELECT
	        nextval('bl_3nf.seq_ce_customers_scd'),
	        COALESCE(NULLIF(t.customer_type, ''), 'n.a.') AS customer_type,
	        COALESCE(NULLIF(t.customer_telephone, ''), 'n.a.') AS customer_telephone,
	        true,
	        v_mod_time,
	        '9999-12-31 23:59:59'::timestamp,
	        t.customer_src_id,
	        COALESCE(t.source_system,  'unknown') AS source_system,
	        COALESCE(t.source_entity, 'unknown') AS source_entity,
	        v_mod_time
	    FROM (
	        SELECT DISTINCT ON (customer_src_id)
	               customer_src_id,
	               customer_type,
	               customer_telephone,
	               source_system,
	               source_entity
	        FROM bl_cl.cleaned_taxi_data
	        WHERE customer_src_id = 'n.a.'
	        ORDER BY customer_src_id, customer_type, customer_telephone
	        LIMIT 1
	    ) t
	    LEFT JOIN bl_3nf.ce_customers_scd ccs
	          ON ccs.customer_src_id = t.customer_src_id
	         AND ccs.is_active = true
	    WHERE ccs.customer_id IS NULL;



        RAISE NOTICE 'Incremental load completed at %', clock_timestamp();
    END IF;

    ----------------------------------------------------------------
    -- Log successful completion
    RAISE NOTICE 'Load completed successfully at %. Total time: % seconds.',
        clock_timestamp(),
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during % load at %: %', load_type, clock_timestamp(), SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;



------------------------------------------------------------------------------------------------------------------------
--ce_bookings

-- Create sequence for booking IDs
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_bookings;

-- Create indexes to optimize queries
CREATE INDEX IF NOT EXISTS idx_booking_src_id ON bl_3nf.ce_bookings (booking_src_id);
CREATE INDEX IF NOT EXISTS idx_booking_datetime ON bl_3nf.ce_bookings (booking_datetime);
CREATE INDEX IF NOT EXISTS idx_booking_src_id_cleaned ON bl_cl.cleaned_taxi_data (booking_src_id);

-- Function to load data into CE_BOOKINGS
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_bookings (load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_mod_time   TIMESTAMP;
BEGIN
    -- Log start of the process
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- Validate load type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    -- Full load: clean table and insert new rows
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE '-- Full load started at % --', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_bookings CASCADE;

        -- Insert new rows from cleaned_taxi_data
  		    	INSERT INTO bl_3nf.ce_bookings (
            booking_id, 
            booking_type, 
            booking_datetime, 
            booking_src_id, 
            source_system, 
            source_entity, 
            update_dt, 
            insert_dt
        )
        SELECT distinct on (ctd.booking_datetime, ctd.booking_src_id)
            nextval('bl_3nf.seq_ce_bookings'),
            COALESCE(NULLIF(ctd.booking_type, ''), 'n.a.'),
            ctd.booking_datetime,
            COALESCE(NULLIF(ctd.booking_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp,
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_bookings cb
        	ON ctd.booking_src_id = cb.booking_src_id
        WHERE cb.booking_src_id IS null
  			and cb.booking_src_id is distinct from ctd.booking_src_id
limit 950000;  

        RAISE NOTICE '-- Full load completed at % --', clock_timestamp();
    END IF;

    -- Incremental load: check for changes and insert/update accordingly
    IF UPPER(load_type) = 'INCREMENTAL' THEN
        RAISE NOTICE '-- Incremental load started at % --', clock_timestamp();
        v_mod_time := clock_timestamp();

        -- A) Update existing records if booking_type or source details changed
		UPDATE bl_3nf.ce_bookings cb
		SET 
		    booking_type = ctd.booking_type,
		    booking_datetime = ctd.booking_datetime,
		    source_system = ctd.source_system,
		    source_entity = ctd.source_entity,
		    update_dt = current_timestamp
		FROM bl_cl.cleaned_taxi_data ctd
		WHERE 
		    cb.booking_src_id = ctd.booking_src_id
		    AND (
		        cb.booking_type       IS DISTINCT FROM ctd.booking_type
		        --OR cb.booking_datetime IS DISTINCT FROM ctd.booking_datetime
		        OR cb.source_system    IS DISTINCT FROM ctd.source_system
		        OR cb.source_entity    IS DISTINCT FROM ctd.source_entity
		    );


        -- B) Insert new records if they do not exist
  		    	INSERT INTO bl_3nf.ce_bookings (
            booking_id, 
            booking_type, 
            booking_datetime, 
            booking_src_id, 
            source_system, 
            source_entity, 
            update_dt, 
            insert_dt
        )
        SELECT distinct on (ctd.booking_datetime, ctd.booking_src_id)
            nextval('bl_3nf.seq_ce_bookings'),
            COALESCE(NULLIF(ctd.booking_type, ''), 'n.a.'),
            ctd.booking_datetime,
            COALESCE(NULLIF(ctd.booking_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp,
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_bookings cb
        	ON ctd.booking_src_id = cb.booking_src_id
        WHERE cb.booking_src_id IS null
  			and cb.booking_src_id is distinct from ctd.booking_src_id;

        RAISE NOTICE '-- Incremental load completed at % --', clock_timestamp();
    END IF;

    -- Log successful completion
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --', 
        clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '-- Error during % load at %: %', load_type, clock_timestamp(), SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;




-----------------------------------------------------------------------------------------------------------------------------
--ce_payments

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_payments;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_payment_src_id_cleaned ON bl_cl.cleaned_taxi_data (payment_src_id, payment_datetime);
CREATE INDEX IF NOT EXISTS idx_payment_src_id_ce_payments ON bl_3nf.ce_payments (payment_src_id, payment_datetime);

-- Function to load data into CE_PAYMENTS
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_payments (load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp(); 
BEGIN
    -- Log start process
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- Check value of load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    ----------------------------------------------
    -- 1. Full load: clean table and insert new rows
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE '-- % -- Full load started. Truncating table...', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_payments CASCADE;
        RAISE NOTICE '-- % -- Full load. Table truncated.', clock_timestamp();

        -- Insert new rows
        RAISE NOTICE '-- % -- Inserting new records ...', clock_timestamp();

        INSERT INTO bl_3nf.ce_payments (
            payment_id, 
            payment_type, 
            payment_datetime, 
            payment_src_id, 
            source_system, 
            source_entity, 
            update_dt, 
            insert_dt
        )
        SELECT 
            nextval('bl_3nf.seq_ce_payments'), 
            COALESCE(NULLIF(ctd.payment_type, ''), 'n.a.'),
            COALESCE(ctd.payment_datetime, NOW()),
            COALESCE(NULLIF(ctd.payment_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'), 
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp, 
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_payments cp
        	ON ctd.payment_src_id = cp.payment_src_id
        WHERE cp.payment_id IS NULL
			and ctd.payment_src_id is distinct from cp.payment_src_id
limit 950000;  

        RAISE NOTICE '-- % -- Full load completed.', clock_timestamp();

    END IF;

    ----------------------------------------------------
    -- 2. Incremental load: check for changes
    IF UPPER(load_type) = 'INCREMENTAL' THEN
        RAISE NOTICE '-- % -- Incremental load started. Checking for changes...', clock_timestamp();
        
        -- Update existing records if values changed
        UPDATE bl_3nf.ce_payments cp
        SET 
            payment_type = ctd.payment_type,
            payment_datetime = ctd.payment_datetime,
            source_system = ctd.source_system,
            source_entity = ctd.source_entity,
            update_dt = current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        WHERE 
			cp.payment_src_id = ctd.payment_src_id
            AND (
              cp.payment_type IS DISTINCT FROM ctd.payment_type
		      --or cp.payment_datetime is distinct from ctd.payment_datetime
              OR cp.source_system IS DISTINCT FROM ctd.source_system
              OR cp.source_entity IS DISTINCT FROM ctd.source_entity
          );


        -- Insert new records if they do not exist
        INSERT INTO bl_3nf.ce_payments (
            payment_id, 
            payment_type, 
            payment_datetime, 
            payment_src_id, 
            source_system, 
            source_entity, 
            update_dt, 
            insert_dt
        )
        SELECT 
            nextval('bl_3nf.seq_ce_payments'), 
            COALESCE(NULLIF(ctd.payment_type, ''), 'n.a.'),
            ctd.payment_datetime, 
            COALESCE(NULLIF(ctd.payment_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp, 
            current_timestamp 
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_payments cp
        	ON ctd.payment_src_id = cp.payment_src_id
        WHERE cp.payment_id IS NULL
			and ctd.payment_src_id is distinct from cp.payment_src_id;

        RAISE NOTICE '-- % -- Incremental load completed.', clock_timestamp();
    END IF;

    -- Log successful completion
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --', clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    -- Handle errors
    WHEN OTHERS THEN
        RAISE NOTICE '-- % -- Error during % load: %', clock_timestamp(), load_type, SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;



-------------------------------------------------------------------------------------------------------

--ce_rates

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_rates;

-- Create indexes
CREATE UNIQUE INDEX if not exists idx_rate_src_id ON bl_3nf.ce_rates (rate_src_id);
CREATE INDEX if not exists idx_rate_src_id_cleaned ON bl_cl.cleaned_taxi_data (rate_src_id);

-- Function to load data into CE_RATES
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_rates (load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();   
    
BEGIN
    -- Log start process
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- Check value of load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    ----------------------------------------------
    -- 1. Full load: clean table and insert new rows
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE '-- % -- Full load started. Truncating table...', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_rates CASCADE;
        RAISE NOTICE '-- % -- Full load. Table truncated.', clock_timestamp();

        -- Insert new rows
        RAISE NOTICE '-- % -- Inserting new records ...', clock_timestamp();

        INSERT INTO bl_3nf.ce_rates (
            rate_id, 
            base_fare, 
            rate_per_mile, 
            rate_src_id, 
            source_system, 
            source_entity, 
            update_dt, 
            insert_dt
        )
        SELECT distinct on (ctd.rate_src_id)
            nextval('bl_3nf.seq_ce_rates'), 
            COALESCE(ctd.base_fare, 0.00),
            COALESCE(ctd.rate_per_mile, 0.00),
            COALESCE(NULLIF(ctd.rate_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'), 
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp, 
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_rates cr
        	ON ctd.rate_src_id = cr.rate_src_id
        WHERE cr.rate_src_id IS NULL
limit 950000;  

    END IF;

    ----------------------------------------------------
    -- 2. Incremental load: check for changes
IF UPPER(load_type) = 'INCREMENTAL' THEN
    RAISE NOTICE '-- % -- Incremental load started. Checking for changes...', clock_timestamp();

    -- Update existing rows if there are changes
    UPDATE bl_3nf.ce_rates cr
    SET 
        base_fare = ctd.base_fare,
        rate_per_mile = ctd.rate_per_mile,
        update_dt = current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    WHERE cr.rate_src_id = ctd.rate_src_id
      AND (cr.base_fare IS DISTINCT FROM ctd.base_fare
           OR cr.rate_per_mile IS DISTINCT FROM ctd.rate_per_mile);

    -- Insert new rows if they do not exist
    INSERT INTO bl_3nf.ce_rates (
        rate_id, 
        base_fare, 
        rate_per_mile, 
        rate_src_id, 
        source_system, 
        source_entity, 
        update_dt, 
        insert_dt
    )
    SELECT 
        nextval('bl_3nf.seq_ce_rates'), 
        COALESCE(ctd.base_fare, 0.00),
        COALESCE(ctd.rate_per_mile, 0.00),
        COALESCE(NULLIF(ctd.rate_src_id, ''), 'n.a.'),
        COALESCE(ctd.source_system, 'unknown'), 
        COALESCE(ctd.source_entity, 'unknown'),
        current_timestamp, 
        current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    LEFT JOIN bl_3nf.ce_rates cr
        ON ctd.rate_src_id = cr.rate_src_id
    WHERE cr.rate_src_id IS NULL;

    RAISE NOTICE '-- % -- Incremental load completed.', clock_timestamp();
END IF;

               
    -- Log successful completion
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --', clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    -- Handle errors
    WHEN OTHERS THEN
        RAISE NOTICE '-- % -- Error during % load: %', clock_timestamp(), load_type, SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;




----------------------------------------------------------------------------------------------------------------------
--ce_locations

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_locations;

-- Function to load data into CE_LOCATIONS
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_locations (load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();

BEGIN
    -- Log start process
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- Check load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    ----------------------------------------------
    -- 1. Full load: clean table and insert new rows
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE '-- % -- Full load started. Truncating table...', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_locations CASCADE;
        RAISE NOTICE '-- % -- Full load. Table truncated.', clock_timestamp();

        -- A) Insert Pickup Locations
        INSERT INTO bl_3nf.ce_locations (
            location_id,
            longitude,
            latitude,
            location_src_id,
            source_system,
            source_entity,
            update_dt,
            insert_dt
        )
        SELECT
            nextval('bl_3nf.seq_ce_locations'),
            COALESCE(ctd.pickup_longitude, 0.00),
            COALESCE(ctd.pickup_latitude, 0.00),
            ctd.location_src_id || '_pickup',
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp,
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_locations cl
        ON (ctd.location_src_id || '_pickup') = cl.location_src_id
        WHERE cl.location_src_id IS NULL;

        -- B) Insert Dropoff Locations
        INSERT INTO bl_3nf.ce_locations (
            location_id,
            longitude,
            latitude,
            location_src_id,
            source_system,
            source_entity,
            update_dt,
            insert_dt
        )
        SELECT
            nextval('bl_3nf.seq_ce_locations'),
            COALESCE(ctd.dropoff_longitude, 0.00),
            COALESCE(ctd.dropoff_latitude, 0.00),
            ctd.location_src_id || '_dropoff',
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp,
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_locations cl
        ON (ctd.location_src_id || '_dropoff') = cl.location_src_id
        WHERE cl.location_src_id IS NULL
limit 950000;  

    END IF;

    ----------------------------------------------------
    -- 2. Incremental load: check for changes
    IF UPPER(load_type) = 'INCREMENTAL' THEN
    RAISE NOTICE '-- % -- Incremental load started. Checking for changes...', clock_timestamp();

    ------------------------------------------------------------------
    -- A) Update pickup locations
    UPDATE bl_3nf.ce_locations loc
    SET 
        longitude = ctd.pickup_longitude,
        latitude = ctd.pickup_latitude,
        update_dt = current_timestamp,
		source_system = ctd.source_system,
		source_entity = ctd.source_entity
    FROM bl_cl.cleaned_taxi_data ctd
    WHERE loc.location_src_id = (ctd.location_src_id || '_pickup')
      AND (
           loc.longitude IS DISTINCT FROM ctd.pickup_longitude
           OR loc.latitude IS DISTINCT FROM ctd.pickup_latitude
		   or loc.source_system is distinct from ctd.source_system
		   or loc.source_entity is distinct from ctd.source_entity	
      );

    ------------------------------------------------------------------
    --  Insert new pickup locations
    INSERT INTO bl_3nf.ce_locations (
        location_id,
        longitude,
        latitude,
        location_src_id,
        source_system,
        source_entity,
        update_dt,
        insert_dt
    )
    SELECT
        nextval('bl_3nf.seq_ce_locations'),
        ctd.pickup_longitude,
        ctd.pickup_latitude,
        (ctd.location_src_id || '_pickup'),
        COALESCE(ctd.source_system, 'unknown'),
        COALESCE(ctd.source_entity, 'unknown'),
        current_timestamp,
        current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    LEFT JOIN bl_3nf.ce_locations loc
        ON loc.location_src_id = (ctd.location_src_id || '_pickup')
    WHERE loc.location_id IS NULL
      AND ctd.pickup_longitude IS NOT NULL
      AND ctd.pickup_latitude IS NOT NULL;

    ------------------------------------------------------------------
    -- B) Update dropoff locations
    UPDATE bl_3nf.ce_locations loc
    SET 
        longitude = ctd.dropoff_longitude,
        latitude = ctd.dropoff_latitude,
        update_dt = current_timestamp,
		source_system = ctd.source_system,
		source_entity = ctd.source_entity
    FROM bl_cl.cleaned_taxi_data ctd
    WHERE loc.location_src_id = (ctd.location_src_id || '_dropoff')
      AND (
           loc.longitude IS DISTINCT FROM ctd.dropoff_longitude
           OR loc.latitude IS DISTINCT FROM ctd.dropoff_latitude
		   or loc.source_system is distinct from ctd.source_system
		   or loc.source_entity is distinct from ctd.source_entity		
      );

    ------------------------------------------------------------------
    --  Insert new dropoff locations
    INSERT INTO bl_3nf.ce_locations (
        location_id,
        longitude,
        latitude,
        location_src_id,
        source_system,
        source_entity,
        update_dt,
        insert_dt
    )
    SELECT
        nextval('bl_3nf.seq_ce_locations'),
        ctd.dropoff_longitude,
        ctd.dropoff_latitude,
        (ctd.location_src_id || '_dropoff'),
        COALESCE(ctd.source_system, 'unknown'),
        COALESCE(ctd.source_entity, 'unknown'),
        current_timestamp,
        current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    LEFT JOIN bl_3nf.ce_locations loc
        ON loc.location_src_id = (ctd.location_src_id || '_dropoff')
    WHERE loc.location_id IS NULL
      AND ctd.dropoff_longitude IS NOT NULL
      AND ctd.dropoff_latitude IS NOT NULL;

    RAISE NOTICE '-- % -- Incremental load completed.', clock_timestamp();
END IF;


    -- Log successful completion
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --',
        clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '-- % -- Error during % load: %', clock_timestamp(), load_type, SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;




---------------------------------------------------------------------------------------------------------------------
--ce_vendor_addresses

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_vendor_addresses;

-- Create indexes
CREATE UNIQUE INDEX if not exists idx_vendor_address_src_id ON bl_3nf.ce_vendor_addresses (vendor_address_src_id);
CREATE INDEX if not exists idx_vendor_address_src_id_cleaned ON bl_cl.cleaned_taxi_data (vendor_address_src_id);


-- Function to load data into CE_VENDOR_ADDRESSES
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_vendor_addresses (load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_vendor_address_src_id varchar;
    v_vendor_street varchar;
    v_vendor_house varchar;
    v_vendor_city varchar;
    v_vendor_country varchar;
    v_vendor_postal_code varchar;
    v_existing_address_id BIGINT;
    v_existing_vendor_street varchar;
    v_existing_vendor_house varchar;
    v_existing_vendor_city varchar;
    v_existing_vendor_country varchar;
    v_existing_vendor_postal_code varchar;
    v_source_system varchar;
    v_source_entity varchar;

BEGIN
    -- Log start process
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- Check load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    ----------------------------------------------
    -- 1. Full load: clean table and insert new rows
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE '-- % -- Full load started. Truncating table...', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_vendor_addresses CASCADE;
        RAISE NOTICE '-- % -- Full load. Table truncated.', clock_timestamp();

        -- Insert new rows
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
        )
        SELECT distinct on (ctd.vendor_address_src_id)          -- there use distinct on because dont wat adding duplicates in table
            nextval('bl_3nf.seq_ce_vendor_addresses'),
            COALESCE(NULLIF(ctd.vendor_street, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_house, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_city, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_country, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_postal_code, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_address_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp,
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_vendor_addresses cva
        ON ctd.vendor_address_src_id = cva.vendor_address_src_id
        WHERE cva.vendor_address_src_id IS NULL
limit 950000;  

    END IF;

    ----------------------------------------------------
    -- 2. Incremental load: check for changes
    IF UPPER(load_type) = 'INCREMENTAL' THEN
    RAISE NOTICE '-- % -- Incremental load started. Checking for changes...', clock_timestamp();

    ----------------------------------------------------------------------------
    -- 1. Update existing rows if any field has changed
    UPDATE bl_3nf.ce_vendor_addresses va
    SET 
        vendor_street = ctd.vendor_street,
        vendor_house = ctd.vendor_house,
        vendor_city = ctd.vendor_city,
        vendor_country = ctd.vendor_country,
        vendor_postal_code = ctd.vendor_postal_code,
        update_dt = current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    WHERE va.vendor_address_src_id = ctd.vendor_address_src_id
      AND (
          va.vendor_street IS DISTINCT FROM ctd.vendor_street
          OR va.vendor_house IS DISTINCT FROM ctd.vendor_house
          OR va.vendor_city IS DISTINCT FROM ctd.vendor_city
          OR va.vendor_country IS DISTINCT FROM ctd.vendor_country
          OR va.vendor_postal_code IS DISTINCT FROM ctd.vendor_postal_code
      );

    ----------------------------------------------------------------------------
    -- 2. Insert new rows if they do not exist
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
    )
    SELECT
        nextval('bl_3nf.seq_ce_vendor_addresses'),
        COALESCE(NULLIF(ctd.vendor_street, ''), 'n.a.') AS vendor_street,
        COALESCE(NULLIF(ctd.vendor_house, ''), 'n.a.') AS vendor_house,
        COALESCE(NULLIF(ctd.vendor_city, ''), 'n.a.') AS vendor_city,
        COALESCE(NULLIF(ctd.vendor_country, ''), 'n.a.') AS vendor_country,
        COALESCE(NULLIF(ctd.vendor_postal_code, ''), 'n.a.') AS vendor_postal_code,
        COALESCE(NULLIF(ctd.vendor_address_src_id, ''), 'n.a.') AS vendor_address_src_id,
        COALESCE(NULLIF(ctd.source_system, ''), 'unknown') AS source_system,
        COALESCE(NULLIF(ctd.source_entity, ''), 'unknown') AS source_entity,
        current_timestamp,
        current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    LEFT JOIN bl_3nf.ce_vendor_addresses va
           ON va.vendor_address_src_id = ctd.vendor_address_src_id
    WHERE va.vendor_address_id IS NULL;  -- no record yet

    RAISE NOTICE '-- % -- Incremental load completed.', clock_timestamp();
END IF;


    -- Log successful completion
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --',
        clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '-- % -- Error during % load: %', clock_timestamp(), load_type, SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;




--------------------------------------------------------------------------------------------------------------------------------
-- ce_vendors

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_vendors;

-- Create indexes
CREATE UNIQUE INDEX if not exists idx_vendor_src_id ON bl_3nf.ce_vendors (vendor_src_id);
CREATE INDEX if not exists idx_vendor_src_id_cleaned ON bl_cl.cleaned_taxi_data (vendor_src_id);

DROP INDEX IF EXISTS dx_vendor_src_id_cleaned;


-- Function to load data into CE_VENDORS
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_vendors (load_type TEXT)
RETURNS VOID AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_vendor_src_id varchar;
    v_vendor_address_id BIGINT;
    v_vendor_name varchar;
    v_vendor_telephone varchar;
    v_existing_vendor_id BIGINT;
    v_existing_vendor_name varchar;
    v_existing_vendor_telephone varchar;
    v_source_system varchar;
    v_source_entity varchar;

BEGIN
    -- Log start process
    RAISE NOTICE '-- Start of % load at % --', load_type, v_start_time;

    -- Check load_type
    IF upper(load_type) NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %, must be ''full'' or ''incremental''.', load_type;
    END IF;

    ----------------------------------------------
    -- 1. Full load: clean table and insert new rows
    IF UPPER(load_type) = 'FULL' THEN
        RAISE NOTICE '-- % -- Full load started. Truncating table...', clock_timestamp();
        TRUNCATE TABLE bl_3nf.ce_vendors CASCADE;
        RAISE NOTICE '-- % -- Full load. Table truncated.', clock_timestamp();

        -- Insert new rows
        INSERT INTO bl_3nf.ce_vendors (
            vendor_id,
            vendor_address_id,
            vendor_name,
            vendor_telephone,
            vendor_src_id,
            source_system,
            source_entity,
            update_dt,
            insert_dt
        )
        SELECT distinct on (ctd.vendor_src_id)       -- we dont want duplicate same rows so we need use distinct on
            nextval('bl_3nf.seq_ce_vendors'),
            coalesce(cva.vendor_address_id, -1),
            COALESCE(NULLIF(ctd.vendor_name, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_telephone, ''), 'n.a.'),
            COALESCE(NULLIF(ctd.vendor_src_id, ''), 'n.a.'),
            COALESCE(ctd.source_system, 'unknown'),
            COALESCE(ctd.source_entity, 'unknown'),
            current_timestamp,
            current_timestamp
        FROM bl_cl.cleaned_taxi_data ctd
        LEFT JOIN bl_3nf.ce_vendor_addresses cva                      -- without vendor_addresses vendor_address_id will be null .. Fk can not be null 
        ON ctd.vendor_address_src_id = cva.vendor_address_src_id
        LEFT JOIN bl_3nf.ce_vendors cv                                  -- no duplicates from vendor
        ON ctd.vendor_src_id = cv.vendor_src_id                     
        WHERE cv.vendor_src_id IS NULL --and cva.vendor_address_src_id is null;
limit 950000;

    END IF;


    ----------------------------------------------------
    -- 2. Incremental load: check for changes
   IF UPPER(load_type) = 'INCREMENTAL' THEN
    RAISE NOTICE '-- % -- Incremental load started. Checking for changes...', clock_timestamp();

    ----------------------------------------------------------------------------
    -- 1. Update existing vendors if any field changed
    ----------------------------------------------------------------------------
    UPDATE bl_3nf.ce_vendors v
    SET 
        vendor_name = ctd.vendor_name,
        vendor_telephone = ctd.vendor_telephone,
        vendor_address_id = cva.vendor_address_id,
        update_dt = current_timestamp
    FROM (
    -- Ensure we take the most recent row per vendor_src_id
    SELECT DISTINCT ON (ctd.vendor_src_id) 
        ctd.vendor_src_id,
        ctd.vendor_name,
        ctd.vendor_telephone,
        ctd.vendor_address_src_id
    FROM bl_cl.cleaned_taxi_data ctd
 ) AS ctd
    left JOIN bl_3nf.ce_vendor_addresses cva
           ON ctd.vendor_address_src_id = cva.vendor_address_src_id
	WHERE v.vendor_src_id = ctd.vendor_src_id
      AND (
           v.vendor_name IS DISTINCT FROM ctd.vendor_name
           OR v.vendor_telephone IS DISTINCT FROM ctd.vendor_telephone
           OR v.vendor_address_id IS DISTINCT FROM cva.vendor_address_id
      );

    ----------------------------------------------------------------------------
    -- 2. Insert new vendors if they do not exist
    ----------------------------------------------------------------------------
    INSERT INTO bl_3nf.ce_vendors (
        vendor_id,
        vendor_address_id,
        vendor_name,
        vendor_telephone,
        vendor_src_id,
        source_system,
        source_entity,
        update_dt,
        insert_dt
    )
    SELECT
        nextval('bl_3nf.seq_ce_vendors'),
        cva.vendor_address_id,
        ctd.vendor_name,
        ctd.vendor_telephone,
        ctd.vendor_src_id,
        COALESCE(NULLIF(ctd.source_system,  ''), 'unknown'),
        COALESCE(NULLIF(ctd.source_entity, ''), 'unknown'),
        current_timestamp,
        current_timestamp
    FROM bl_cl.cleaned_taxi_data ctd
    LEFT JOIN bl_3nf.ce_vendor_addresses cva
           ON ctd.vendor_address_src_id = cva.vendor_address_src_id
    LEFT JOIN bl_3nf.ce_vendors v
           ON v.vendor_src_id = ctd.vendor_src_id
    WHERE v.vendor_id IS NULL;

    RAISE NOTICE '-- % -- Incremental load completed.', clock_timestamp();
END IF;


    -- Log successful completion
    RAISE NOTICE '-- Load completed successfully at %. Total time: % seconds. --',
        clock_timestamp(), EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '-- % -- Error during % load: %', clock_timestamp(), load_type, SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql;







--------------------------------------------------------------------------------------------------------------------------------
-- ce_taxi_trips

-- Create sequence
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_taxi_trip;

-- create index for cleaned_taxi_data table
CREATE INDEX IF NOT EXISTS idx_ctd_trip_src_id ON bl_cl.cleaned_taxi_data (trip_src_id);
CREATE INDEX IF NOT EXISTS idx_ctd_vendor_src_id ON bl_cl.cleaned_taxi_data (vendor_src_id);
CREATE INDEX IF NOT EXISTS idx_ctd_booking_src_id ON bl_cl.cleaned_taxi_data (booking_src_id);
CREATE INDEX IF NOT EXISTS idx_ctd_customer_src_id ON bl_cl.cleaned_taxi_data (customer_src_id);
CREATE INDEX IF NOT EXISTS idx_ctd_promo_src_id ON bl_cl.cleaned_taxi_data (promo_src_id);
CREATE INDEX IF NOT EXISTS idx_ctd_rate_src_id ON bl_cl.cleaned_taxi_data (rate_src_id);
CREATE INDEX IF NOT EXISTS idx_ctd_pickup_location ON bl_cl.cleaned_taxi_data (pickup_longitude, pickup_latitude);
CREATE INDEX IF NOT EXISTS idx_ctd_dropoff_location ON bl_cl.cleaned_taxi_data (dropoff_longitude, dropoff_latitude);
CREATE INDEX IF NOT EXISTS idx_location_long_lat ON bl_3nf.ce_locations (longitude, latitude);

-- create index for 3NF tables
CREATE INDEX idx_cleaned_taxi_trip_src_id ON bl_cl.cleaned_taxi_data (trip_src_id);
CREATE INDEX IF NOT EXISTS idx_vendors_vendor_src_id ON bl_3nf.ce_vendors (vendor_src_id);
CREATE INDEX IF NOT EXISTS idx_bookings_booking_src_id ON bl_3nf.ce_bookings (booking_src_id);
CREATE INDEX IF NOT EXISTS idx_customers_customer_src_id ON bl_3nf.ce_customers_scd (customer_src_id, is_active);
CREATE INDEX IF NOT EXISTS idx_promotions_promo_src_id ON bl_3nf.ce_promotions (promo_src_id);
CREATE INDEX IF NOT EXISTS idx_rates_rate_src_id ON bl_3nf.ce_rates (rate_src_id);
CREATE INDEX IF NOT EXISTS idx_locations_pickup ON bl_3nf.ce_locations (longitude, latitude);
CREATE INDEX IF NOT EXISTS idx_locations_dropoff ON bl_3nf.ce_locations (longitude, latitude);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ce_taxi_trips_trip_src_id ON bl_3nf.ce_taxi_trips (trip_src_id);


--func
CREATE OR REPLACE FUNCTION bl_3nf.load_ce_taxi_trips(load_type TEXT)
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
	--TRUNCATE TABLE bl_3nf.ce_taxi_trips CASCADE;
	
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
	INSERT INTO bl_3nf.ce_taxi_trips (
	    trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
	    pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
	    distance_miles, trip_duration, passenger_count, trip_src_id,
	    source_system, source_entity, customer_start_dt, update_dt, insert_dt
	)
	SELECT DISTINCT ON (ctd.trip_src_id)
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
		--and ctd.trip_src_id > (SELECT COALESCE(min(trip_src_id), '0') FROM bl_3nf.ce_taxi_trips)
        limit 100000;
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
	UPDATE bl_3nf.ce_taxi_trips ctt
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
    FROM bl_cl.cleaned_taxi_data_2016_01_start ctd
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
	INSERT INTO bl_3nf.ce_taxi_trips (
	    trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
	    pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
	    distance_miles, trip_duration, passenger_count, trip_src_id,
	    source_system, source_entity, customer_start_dt, update_dt, insert_dt
	)
	SELECT DISTINCT ON (ctd.trip_src_id)
	    nextval('bl_3nf.seq_ce_taxi_trips') AS trip_id,                                     
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
	FROM bl_cl.cleaned_taxi_data_2016_01_start ctd
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
	    AND ctd.passenger_count > 0;

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
-----------------------------------------------------------------------------------------------------------------








  
----------------------------------------------------------------------------------------------------------------
-- Run full/incremental loads 

select bl_3nf.load_ce_promotions('full');            
select bl_3nf.load_ce_customers_scd('full');
select bl_3nf.load_ce_bookings('full'); 
select bl_3nf.load_ce_payments('full');
select bl_3nf.load_ce_rates('full');
select bl_3nf.load_ce_locations('full');
select bl_3nf.load_ce_vendor_addresses('full');
select bl_3nf.load_ce_vendors('full');
select bl_3nf.load_ce_taxi_trips('full');


select bl_3nf.load_ce_promotions('incremental');
select bl_3nf.load_ce_customers_scd('incremental');
select bl_3nf.load_ce_bookings('incremental'); 
select bl_3nf.load_ce_payments('incremental');
select bl_3nf.load_ce_rates('incremental');
select bl_3nf.load_ce_locations('incremental');
select bl_3nf.load_ce_vendor_addresses('incremental');
select bl_3nf.load_ce_vendors('incremental');
select bl_3nf.load_ce_taxi_trips('incremental');


select * from bl_cl.cleaned_taxi_data;

select * from bl_3nf.ce_promotions;
select * from bl_3nf.ce_customers_scd;
select * from bl_3nf.ce_bookings;
select * from bl_3nf.ce_payments;
select * from bl_3nf.ce_rates;
select * from bl_3nf.ce_locations;
select * from bl_3nf.ce_vendor_addresses;
select * from bl_3nf.ce_vendors;
select * from bl_3nf.ce_taxi_trips;



select count(*) from bl_3nf.ce_promotions;        -- 4        d.r.      
select count(*) from bl_3nf.ce_customers_scd;     --383 765   d.r  
select count(*) from bl_3nf.ce_bookings;          --275 005   d.r. 
select count(*) from bl_3nf.ce_payments;          --995 336   d.r.   -950 000
select count(*) from bl_3nf.ce_rates;             -- 2        d.r.
select count(*) from bl_3nf.ce_locations;         --1 990 672 d.r. -1 945 336
select count(*) from bl_3nf.ce_vendor_addresses;  -- 2        d.r.
select count(*) from bl_3nf.ce_vendors;           -- 2        d.r.
select count(*) from bl_3nf.ce_taxi_trips;




--TRUNCATE TABLE bl_cl.cleaned_taxi_data;
TRUNCATE TABLE bl_3nf.ce_promotions cascade;
TRUNCATE TABLE bl_3nf.ce_customers_scd cascade;
TRUNCATE TABLE bl_3nf.ce_bookings cascade;
TRUNCATE TABLE bl_3nf.ce_payments cascade;
TRUNCATE TABLE bl_3nf.ce_rates cascade;
TRUNCATE TABLE bl_3nf.ce_locations cascade;
TRUNCATE TABLE bl_3nf.ce_vendor_addresses cascade;
TRUNCATE TABLE bl_3nf.ce_vendors cascade;
TRUNCATE TABLE bl_3nf.ce_taxi_trips cascade;



-------------------------------------------------------------------------------------------------
--New row add, update, delete

-- delete new row
delete from bl_cl.cleaned_taxi_data 
where trip_src_id = 'trip-9000';

-- insert new row
INSERT INTO bl_cl.cleaned_taxi_data (
    trip_src_id,
    vendor_src_id,
    vendor_name,
    vendor_address_src_id,
    vendor_street,
    vendor_house,
    vendor_city,
    vendor_country,
    vendor_postal_code,
    vendor_telephone,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    location_src_id,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    distance_miles,
    trip_duration,
    booking_src_id,
    booking_type,
    booking_datetime,
    rate_src_id,
    base_fare,
    rate_per_mile,
    payment_src_id,
    payment_type,
    payment_datetime,
    customer_src_id,
    customer_type,
    customer_telephone,
    promo_src_id,
    promo_code,
    discount_percentage,
    source_system,
    source_entity
)
VALUES 
(
    'trip-9000',                        -- trip_src_id
    'vendor-789',                       -- vendor_src_id
    'Example Vendor',                   -- vendor_name
    'address-789',                      -- vendor_address_src_id
    'Second Avenue',                    -- vendor_street
    '20B',                              -- vendor_house
    'Los Angeles',                      -- vendor_city
    'USA',                              -- vendor_country
    '90001',                            -- vendor_postal_code
    '0987654321',                       -- vendor_telephone
    '2016-03-15 09:30:00.000',      -- pickup_datetime
    '2016-03-15 09:45:00.000',      -- dropoff_datetime
    3,                                  -- passenger_count
    'loc-777',                          -- location_src_id
    34.56,                              -- pickup_longitude
    78.90,                              -- pickup_latitude
    45.67,                              -- dropoff_longitude
    89.01,                              -- dropoff_latitude
    8.0,                                -- distance_miles
    15,                                 -- trip_duration (в минутах)
    'book-98765',                       -- booking_src_id
    'mobile',                           -- booking_type
    '2017-06-15 09:25:00.000 +0200',      -- booking_datetime
    'rate-54321',                       -- rate_src_id
    120.00,                             -- base_fare
    3.75,                               -- rate_per_mile
    'payment-55555',                    -- payment_src_id
    'cash',                             -- payment_type
    '2016-03-15 09:50:00.000 +0200',      -- payment_datetime
    'cust-888',                         -- customer_src_id
    'premium',                          -- customer_type
    '11223344',                         -- customer_telephone
    'promo-321',                        -- promo_src_id
    'DISCOUNT15',                       -- promo_code
    15,                                 -- discount_percentage
    'demo_system',                      -- source_system
    'demo_entity'                       -- source_entity
);


-- update row
UPDATE bl_cl.cleaned_taxi_data
SET
    vendor_src_id        = 'vendor-789',
    vendor_name          = 'Test Vendor',
    vendor_address_src_id= 'address-789',
    vendor_street        = 'Main Street',
    vendor_house         = '10',
    vendor_city          = 'New York',
    vendor_country       = 'USA',
    vendor_postal_code   = '10001',
    vendor_telephone     = '1234567890',
    pickup_datetime      = '2016-03-26 13:48:00.000 +0300',
    dropoff_datetime     = '2016-03-26 13:55:00.000 +0300',
    passenger_count      = 2,
    location_src_id      = 'loc-777',
    pickup_longitude     = 55.22,
    pickup_latitude      = 555.22,
    dropoff_longitude    = 66.22,
    dropoff_latitude     = 666.22,
    distance_miles       = 5.5,
    trip_duration        = 30,
    booking_src_id       = 'book-98765',
    booking_type         = 'online',
    booking_datetime     = '2016-03-26 13:46:00.000 +0300',
    rate_src_id          = 'rate-54321',
    base_fare            = 100.00,
    rate_per_mile        = 2.50,
    payment_src_id       = 'payment-55555',
    payment_type         = 'card',
    payment_datetime     = '2016-05-26 13:56:00.000 +0300',
    customer_src_id      = 'cust-888',
    customer_type        = 'test',
    customer_telephone   = '9999888',
    promo_src_id         = 'promo-321',
    promo_code           = 'SAVE20',
    discount_percentage  = 20,
    source_system        = 'test',
    source_entity        = 'test'
WHERE trip_src_id        = 'trip-9000';




--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------
-- ce_taxi_trips
	
	-- B) insert values in table
	WITH -- I choose columns that I need it is faster way to fill table
	vendor_ids AS (
	    SELECT DISTINCT vendor_src_id, vendor_id
	    FROM bl_3nf.ce_vendors
	),
	booking_ids AS (
	    SELECT DISTINCT booking_src_id, booking_id
	    FROM bl_3nf.ce_bookings
	),
	customer_ids AS (
	    SELECT DISTINCT customer_src_id, customer_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    WHERE is_active = true
	),
	promo_ids AS (
	    SELECT DISTINCT promo_src_id, promo_id
	    FROM bl_3nf.ce_promotions
	),
	payment_ids AS (
	    SELECT DISTINCT payment_src_id, payment_id
	    FROM bl_3nf.ce_payments
	),
	rate_ids AS (
	    SELECT DISTINCT rate_src_id, rate_id
	    FROM bl_3nf.ce_rates
	),
	pickup_locations AS (
	    SELECT DISTINCT longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	),
	dropoff_locations AS (
	    SELECT DISTINCT longitude, latitude, location_id
	    FROM bl_3nf.ce_locations
	)
	
	
	
	-- Создаем временные таблицы для справочников, если их размер не очень велик:
CREATE TEMP TABLE bl_3nf.tmp_vendor_ids AS
SELECT DISTINCT ON (vendor_src_id, vendor_telephone) vendor_src_id, vendor_id
FROM bl_3nf.ce_vendors;

CREATE TEMP TABLE bl_3nf.tmp_booking_ids AS
SELECT DISTINCT ON (booking_src_id) booking_src_id, booking_id
FROM bl_3nf.ce_bookings;

CREATE TEMP TABLE bl_3nf.tmp_customer_ids AS
SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_src_id, customer_id, start_dt
FROM bl_3nf.ce_customers_scd
WHERE is_active = true;

CREATE TEMP TABLE bl_3nf.tmp_promo_ids AS
SELECT DISTINCT ON (promo_src_id) promo_src_id, promo_id
FROM bl_3nf.ce_promotions;

CREATE TEMP TABLE bl_3nf.tmp_payment_ids AS
SELECT DISTINCT ON (payment_src_id) payment_src_id, payment_id
FROM bl_3nf.ce_payments;

CREATE TEMP TABLE bl_3nf.tmp_rate_ids AS
SELECT DISTINCT ON (rate_src_id) rate_src_id, rate_id
FROM bl_3nf.ce_rates;

CREATE TEMP TABLE bl_3nf.tmp_pickup_locations AS
SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
FROM bl_3nf.ce_locations;

CREATE TEMP TABLE bl_3nf.tmp_dropoff_locations AS
SELECT DISTINCT ON (longitude, latitude) longitude, latitude, location_id
FROM bl_3nf.ce_locations;

	
	
	INSERT INTO bl_3nf.ce_taxi_trips (
	    trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
	    pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
	    distance_miles, trip_duration, passenger_count, trip_src_id,
	    source_system, source_entity, customer_start_dt, update_dt, insert_dt
	)
	SELECT DISTINCT
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
	FROM bl_cl.cleaned_taxi_data ctd
	LEFT JOIN bl_3nf.tmp_vendor_ids v ON ctd.vendor_src_id = v.vendor_src_id
	LEFT JOIN tmp_booking_ids b ON ctd.booking_src_id = b.booking_src_id
	LEFT JOIN (
	    SELECT DISTINCT ON (customer_src_id, customer_telephone) customer_id, customer_src_id, start_dt
	    FROM bl_3nf.ce_customers_scd
	    --WHERE is_active = true
	) c ON ctd.customer_src_id = c.customer_src_id
	LEFT JOIN tmp_promo_ids p ON ctd.promo_src_id = p.promo_src_id
	LEFT JOIN tmp_payment_ids py ON ctd.payment_src_id = py.payment_src_id
	LEFT JOIN tmp_rate_ids r ON ctd.rate_src_id = r.rate_src_id
	LEFT JOIN tmp_pickup_locations pl 
	    ON ctd.pickup_longitude IS NOT NULL 
	    AND ctd.pickup_latitude IS NOT NULL
	    AND ctd.pickup_longitude = pl.longitude 
	    AND ctd.pickup_latitude = pl.latitude
	LEFT JOIN tmp_dropoff_locations dl 
	    ON ctd.dropoff_longitude IS NOT NULL 
	    AND ctd.dropoff_latitude IS NOT NULL
	    AND ctd.dropoff_longitude = dl.longitude 
	    AND ctd.dropoff_latitude = dl.latitude
--LEFT JOIN bl_3nf.ce_taxi_trips t
	   -- ON ctd.trip_src_id = t.trip_src_id
--WHERE t.trip_src_id is null 
	    --AND (ctd.pickup_datetime < ctd.dropoff_datetime 
	       --OR ctd.pickup_datetime IS NULL 
	       --OR ctd.dropoff_datetime IS NULL)
	    --AND ctd.passenger_count > 0
limit 1000;



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

;

---------------------------
CREATE INDEX idx_tmp_vendor ON tmp_vendor_ids (vendor_src_id);
CREATE INDEX idx_tmp_booking ON tmp_booking_ids (booking_src_id);
CREATE INDEX idx_tmp_customer ON tmp_customer_ids (customer_src_id);
CREATE INDEX idx_tmp_promo ON tmp_promo_ids (promo_src_id);
CREATE INDEX idx_tmp_payment ON tmp_payment_ids (payment_src_id);
CREATE INDEX idx_tmp_rate ON tmp_rate_ids (rate_src_id);
CREATE INDEX idx_tmp_locations ON tmp_locations (longitude, latitude);



-- Создаем временные таблицы для справочников, если их размер не очень велик:
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

-- Вставка данных в основную таблицу ce_taxi_trips
INSERT INTO bl_3nf.ce_taxi_trips (
    trip_id, vendor_id, booking_id, customer_id, promo_id, payment_id, rate_id,
    pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime,
    distance_miles, trip_duration, passenger_count, trip_src_id,
    source_system, source_entity, customer_start_dt, update_dt, insert_dt
)
SELECT DISTINCT
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
FROM bl_cl.cleaned_taxi_data ctd
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
LEFT JOIN bl_3nf.ce_taxi_trips t
	   ON ctd.trip_src_id = t.trip_src_id
WHERE t.trip_src_id is null 
	    AND (ctd.pickup_datetime < ctd.dropoff_datetime 
	       OR ctd.pickup_datetime IS NULL 
	       OR ctd.dropoff_datetime IS NULL)
	    AND ctd.passenger_count > 0    
limit 1000 offset 0;





SET log_statement = 'none';

SHOW log_directory;

SELECT pid, usename, query, state, wait_event_type, wait_event, now() - query_start AS duration
FROM pg_stat_activity
WHERE state NOT IN ('idle')
ORDER BY duration DESC;

