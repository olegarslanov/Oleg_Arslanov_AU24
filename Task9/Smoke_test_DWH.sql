-- Extended Smoke Test

-- 1. row count checks
SELECT 
  (SELECT COUNT(*) FROM public.s1_sales) + (select count(*) from public.s2_client_sales) AS source_count,
  (SELECT COUNT(*) FROM public.lnd_s1_sales) + (select count(*) from public.lnd_s2_client_sales) AS lnd_count,
  (SELECT COUNT(*) FROM public.dwh_sales) AS dwh_count,
  (SELECT COUNT(*) FROM public.dm_main_dashboard) AS dm_count;


-- 2. sanity check:
-- not NULL
SELECT COUNT(*) FROM public.dwh_sales WHERE sale_id IS null;
SELECT COUNT(*) FROM public.dwh_sales WHERE client_id is null and channel_id is null and product_id is null;

SELECT COUNT(*) FROM public.dwh_clients WHERE client_id IS NULL;
SELECT COUNT(*) FROM public.dwh_clients WHERE client_src_id is null and first_name is null and last_name is null;

SELECT COUNT(*) FROM public.dwh_products WHERE product_id IS NULL;
SELECT COUNT(*) FROM public.dwh_products WHERE product_src_id IS null and product_name is null;

SELECT COUNT(*) FROM public.dwh_channels WHERE channel_id IS NULL;
SELECT COUNT(*) FROM public.dwh_channels WHERE channel_src_id IS null and channel_name is null and location_id is null;

SELECT COUNT(*) FROM public.dwh_locations WHERE location_id IS NULL;
SELECT COUNT(*) FROM public.dwh_locations WHERE location_src_id IS null and location_name is null;

SELECT COUNT(*) FROM public.dm_main_dashboard WHERE id IS NULL;

-- data types
-- integer format is correct
SELECT *
FROM dwh_sales
WHERE quantity IS NULL OR quantity < 0;

SELECT *
FROM dwh_products
WHERE product_cost IS NULL OR product_cost < 0;

SELECT *
FROM dm_main_dashboard
WHERE total_cost IS NULL OR total_cost < 0;

-- email format is correct
SELECT *
FROM public.dwh_clients
WHERE email IS NOT NULL
  AND email !~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'; --email format check
  
SELECT *
FROM public.dm_main_dashboard
WHERE email IS NOT NULL
  AND email !~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'; --email format check

-- telephone number is correct
SELECT *
FROM public.dwh_clients
WHERE phone_number IS NOT NULL
  AND phone_number !~ '^\+?[0-9]{10,15}$';

SELECT *
FROM public.dm_main_dashboard
WHERE phone_number IS NOT NULL
  AND phone_number !~ '^\+?[0-9]{10,15}$';
  
--date data type  
SELECT *
FROM public.dwh_clients
WHERE valid_from NOT BETWEEN '1900-01-01' AND CURRENT_DATE;

SELECT *
FROM public.dwh_clients
WHERE valid_to NOT BETWEEN '1900-01-01' AND CURRENT_DATE;

SELECT *
FROM public.dwh_sales
WHERE order_completed NOT BETWEEN '1900-01-01' AND CURRENT_DATE;


-- 3. Check data mart aggregation
SELECT 
  COUNT(DISTINCT id) AS orders,
  SUM(total_cost) AS total_cost,
  AVG(total_cost) AS avg_order_cost,
  MIN(total_cost) AS min_order_cost,
  MAX(total_cost) AS max_order_cost
FROM public.dm_main_dashboard;


-- * Ensure uniques

SELECT COUNT(*) 
FROM public.dwh_sales GROUP BY sale_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_clients GROUP BY client_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_products GROUP BY product_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_locations GROUP BY location_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_channels GROUP BY channel_id HAVING COUNT(*) > 1;


SELECT COUNT(*) 
FROM public.dwh_clients GROUP BY client_src_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_products GROUP BY product_src_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_locations GROUP BY location_src_id HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM public.dwh_channels GROUP BY channel_src_id HAVING COUNT(*) > 1;





