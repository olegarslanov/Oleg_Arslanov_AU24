--create db
CREATE DATABASE dwh_hw_db
WITH 
OWNER = dwh_hw_user
ENCODING = 'UTF8'
LC_COLLATE = 'en_US.UTF-8'
LC_CTYPE = 'en_US.UTF-8'
TABLESPACE = pg_default
CONNECTION LIMIT = -1;



--create role
CREATE ROLE dwh_hw_user WITH
LOGIN
NOSUPERUSER
NOCREATEDB
CREATEROLE
INHERIT
NOREPLICATION
CONNECTION LIMIT -1;

--need to create password, because Dbeaver can not connection without password
ALTER ROLE dwh_hw_user WITH PASSWORD '1983';

--create privileges for all table for user dwh_hw_user
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO dwh_hw_user;








---------------------------
--1.Testing (by my Test Cases in TestRail)

--Test [C2263] for more each sale_id is unique 
SELECT sale_id, COUNT(*)
FROM dwh_sales
GROUP BY sale_id
HAVING COUNT(*) > 1;

--result: no errors

Check NULLs is source systems:


--Test [C2478] Id is not NULL
SELECT * FROM s1_clients WHERE client_id IS NULL;

SELECT * FROM s1_channels WHERE channel_id IS NULL;

SELECT * FROM s1_sales WHERE client_id is NULL and channel_id IS NULL and product_id IS NULL ;

SELECT * FROM s1_products WHERE product_id IS NULL ;

SELECT * FROM s2_clients WHERE client_id IS NULL;

SELECT * FROM s2_client_sales WHERE client_id is NULL and channel_id IS NULL and product_id is NULL;

SELECT * FROM s2_channels WHERE channel_id is NULL and location_id IS NULL ;

SELECT * FROM s2_locations WHERE location_id IS NULL ;

--result: no errors

--Test [C2653] ensure that at least one product with a non-null product_id 
SELECT COUNT(*)
FROM dwh_clients
WHERE client_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL;

SELECT COUNT(*)
FROM dwh_sales
WHERE sale_id IS NOT NULL AND client_id IS NOT NULL AND product_id IS NOT NULL;

SELECT COUNT(*)
FROM dwh_products
WHERE product_id IS NOT NULL AND product_name IS NOT NULL;

--result: no errors


--Test [C3314] Total row count from Source Systems should exactly match the row count in dwh_sales (consinstency)
SELECT
    (SELECT COUNT(*) FROM s1_sales) +
    (SELECT COUNT(*) FROM s2_client_sales) AS total_source_count;

SELECT COUNT(*) AS total_dwh_count
FROM dwh_sales;

--result: no errors


--Test [C2375] identify any records in the DWH_CLIENTS table where the client’s name or surname is incomplete or improperly populated
SELECT client_id
FROM DWH_CLIENTS
WHERE first_name IS NULL
   OR TRIM(first_name) = ''
   OR last_name IS NULL
   OR TRIM(last_name) = '';

--result: no errors


--Test [C2382] The counts from the combined source tables (S1_CLIENTS and S2_CLIENT_SALES) should match or reasonably align with the counts in DWH_CLIENTS
SELECT
    COUNT(DISTINCT subquery.client_id) AS source_clients_count,
    COUNT(DISTINCT dwh.client_id) AS dwh_clients_count,
    COUNT(DISTINCT subquery.client_id) - COUNT(DISTINCT dwh.client_id) AS difference
FROM (
    SELECT client_id FROM S1_CLIENTS
    UNION
    SELECT client_id FROM S2_CLIENT_SALES
) AS subquery
FULL JOIN DWH_CLIENTS dwh ON subquery.client_id = dwh.client_id::varchar;

--result: no errors



--Test [C3307] 

--Run the following query to get total amount from source:
select round(sum(total_amount), 1)
from (
   SELECT sum(ss.units::numeric * sp.cost::numeric) as total_amount
   FROM s1_sales ss
   left join S1_PRODUCTS sp on ss.product_id = sp.product_id
UNION ALL
   SELECT sum(product_amount::numeric * product_price::numeric) as total_amount
   FROM s2_client_sales
) as src_totals;

--Run the following query in the DWH layer:
SELECT SUM(quantity * product_cost)
FROM dwh_sales ds
left join dwh_products dp on ds.product_id = dp.product_id;

--Run the following query in the Data Mart layer:
SELECT SUM(total_cost)
FROM dm_main_dashboard;

--result : we found it my first test that found bug... DWH layer total and DM layer total sales is different 



--Test [C2286]

--Run a query to check for orphan client_id in dwh_sales (sales referencing a non-existent client).
SELECT sale_id
FROM dwh_sales s
LEFT JOIN dwh_clients c ON s.client_id = c.client_id
WHERE c.client_id IS NULL;

--Check for invalid product_id in dwh_sales:
SELECT sale_id
FROM dwh_sales s
LEFT JOIN dwh_products p ON s.product_id = p.product_id
WHERE p.product_id IS NULL;

--Check for invalid channel_id in dwh_sales:
SELECT sale_id
FROM dwh_sales s
LEFT JOIN dwh_channels ch ON s.channel_id = ch.channel_id
WHERE ch.channel_id IS NULL;

--Count how many sales have all three dimensions missing :
SELECT COUNT(*)
FROM dwh_sales s
LEFT JOIN dwh_clients c ON s.client_id = c.client_id
LEFT JOIN dwh_products p ON s.product_id = p.product_id
LEFT JOIN dwh_channels ch ON s.channel_id = ch.channel_id
WHERE c.client_id IS NULL and p.product_id IS NULL and ch.channel_id IS NULL;


--result: no errors



---------------------------
--2. Testing (try to find additional bugs/issues, not by Test cases plan)


-- first we check default values

--check for is_valid value 'Y', when valid_to > 2021-01-20
select * 
from dwh_clients
where valid_to > '2021-01-20' and is_valid = 'N';

--result: no errors


--check valid_to from s1_clients is 2100-01-01
select count (sc.client_id)
from dwh_clients dc 
join s1_clients sc on sc.client_id = dc.client_src_id
where valid_to != '2100-01-01';

select count(*)
from dwh_clients dc;

--result: error 700 entities from 780


--check valid_from from s1_clients is 2000-01-01
select count (sc.client_id)
from dwh_clients dc 
join s1_clients sc on sc.client_id = dc.client_src_id
where valid_from != '2000-01-01';

select count(*)
from dwh_clients dc;

--result: no errors


--check location_src_id from s1 system is N/A
select count (location_src_id)
from dwh_locations dl 
join s1_channels sc on sc.channel_location = dl.location_name
where location_src_id != 'N/A';

--select * from s1_channels;
--select * from dwh_locations;

--result: error 15 entities is not N/A fro ms1 system


--check if middle_name value 'N/A' always from s2 system
select count(*) 
from dwh_clients dc
join s2_clients sc on sc.client_id = dc.client_src_id
where middle_name != 'N/A';

--result : no errors


--check if phone_number value phone_code || phone_number always from s2 system
select count(*) 
from dwh_clients dc
join s2_clients sc on sc.client_id = dc.client_src_id
where dc.phone_number != sc.phone_code || sc.phone_number;

--select sc.phone_code || sc.phone_number from s2_clients sc;

--result : no errors


--PK/FK not NULL
select *
from dwh_clients dc 
where client_id is null or client_src_id is null;

select *
from dwh_sales 
where sale_id is null or client_id is null or channel_id is null or product_id is null;

select *
from dwh_products
where product_id is null or product_src_id is null;

select *
from dwh_channels
where channel_id is null or channel_src_id is null or location_id is null;

select *
from dwh_locations
where location_id is null or location_src_id is null;

--result: no errors


--Check for distinct values in source
--check distinct values from s1 system
select channel_id, count(*)
from s1_channels
group by channel_id
having count(*) > 1;

select client_id, count(*)
from s1_clients
group by client_id
having count(*) > 1;

select client_id, channel_id, sale_date, units, product_id, purchase_date, count(*)
from s1_sales
group by client_id, channel_id, sale_date, units, product_id, purchase_date
having count(*) > 1;

select product_id, count(*)
from s1_products sp 
group by product_id
having count(*) > 1;

--check distinct values from s2 system
select client_id, count(*)
from s2_clients
group by client_id
having count(*) > 1;

select client_id, sold_date, product_id, count(*)
from s2_client_sales
group by client_id, sold_date, product_id
having count(*) > 1;

select channel_id, count(*)
from s2_channels
group by channel_id
having count(*) > 1;


select location_id, count(*)
from s2_locations
group by location_id
having count(*) > 1;


--result: no errors


--check distinct values from DWH system
select client_src_id, count(*)
from dwh_clients
group by client_src_id
having count(*) > 1;

select client_id, count(*)
from dwh_clients
group by client_id
having count(*) > 1;

select sale_id, count(*)
from dwh_sales
group by sale_id
having count(*) > 1;

select product_id, count(*)
from dwh_products
group by product_id
having count(*) > 1;

select product_src_id, count(*)
from dwh_products
group by product_src_id
having count(*) > 1;

select channel_id, count(*)
from dwh_channels
group by channel_id
having count(*) > 1;

select channel_src_id, count(*)
from dwh_channels
group by channel_src_id
having count(*) > 1;

select location_id, count(*)
from dwh_locations
group by location_id
having count(*) > 1;

select location_src_id, count(*)
from dwh_locations
group by location_src_id
having count(*) > 1;


--check dwh_locations location_name column

-- distinct_s1 + distinct_s2 с distinct_dwh
SELECT
  dwh_count,
  s1_count,
  s2_count,
  (s1_count + s2_count) AS total_sources,
  CASE
    WHEN dwh_count = s1_count + s2_count THEN 'MATCH'
    ELSE 'MISMATCH'
  END AS result
FROM (
  SELECT
    (SELECT COUNT(DISTINCT TRIM(LOWER(location_name)))
     FROM dwh_locations
     WHERE location_name IS NOT NULL) AS dwh_count,

    (SELECT COUNT(DISTINCT TRIM(LOWER(channel_location)))
     FROM s1_channels
     WHERE channel_location IS NOT NULL) AS s1_count,

    (SELECT COUNT(DISTINCT TRIM(LOWER(location_name)))
     FROM s2_locations
     WHERE location_name IS NOT NULL) AS s2_count
) AS counts;


SELECT sc.channel_location
FROM s1_channels sc
WHERE sc.channel_location NOT IN (
    SELECT dl.location_name 
    FROM dwh_locations dl
    WHERE dl.location_name IS NOT NULL
);

SELECT sl.location_name
FROM s2_locations sl
WHERE sl.location_name NOT IN (
    SELECT dl.location_name 
    FROM dwh_locations dl
    WHERE dl.location_name IS NOT NULL
);


--result: no errors


-- 
--check rows transfer from source to landing system--

--s1_clients to lnd_s1_clients
select count(*) from lnd_s1_clients;
select count(*) from s1_clients;

SELECT * FROM lnd_s1_clients
EXCEPT ALL
SELECT * FROM s1_clients;

SELECT * FROM s1_clients
EXCEPT ALL
SELECT * FROM lnd_s1_clients;

SELECT client_id, COUNT(*) as duplicate_count
FROM s1_clients
GROUP BY client_id 
HAVING COUNT(*) > 1;

SELECT client_id, COUNT(*) as duplicate_count
FROM lnd_s1_clients
GROUP BY client_id 
HAVING COUNT(*) > 1;

--result: error 11 rows too much in landing source (I think dublicated by etl process)


--s1_channels to lnd_s1_channels
select count(*) from lnd_s1_channels;
select count(*) from s1_channels;

SELECT * FROM lnd_s1_channels
EXCEPT ALL
SELECT * FROM s1_channels;

SELECT * FROM s1_channels
EXCEPT ALL
SELECT * FROM lnd_s1_channels;

--result: no errors


--s1_sales to lnd_s1_sales
select count(*) from lnd_s1_sales;
select count(*) from s1_sales;

SELECT * FROM lnd_s1_sales
EXCEPT ALL
SELECT * FROM s1_sales;

SELECT * FROM s1_sales
EXCEPT ALL
SELECT * FROM lnd_s1_sales;

select count(distinct client_id) from s1_sales;
select count(distinct client_id) from lnd_s1_sales;


SELECT client_id, channel_id, sale_date, product_id, COUNT(*) AS duplicate_count
FROM s1_sales
GROUP BY client_id, channel_id, sale_date, product_id
HAVING COUNT(*) > 1;

--result: error 176 rows missing


--s1_products to lnd_s1_products
select count(*) from lnd_s1_products;
select count(*) from s1_products;

SELECT * FROM lnd_s1_products
EXCEPT ALL
SELECT * FROM s1_products;

SELECT * FROM s1_products
EXCEPT ALL
SELECT * FROM lnd_s1_products;

--result: no errors

--
--s2_clients to lnd_s2_clients
select count(*) from lnd_s2_clients;
select count(*) from s2_clients;

SELECT * FROM lnd_s2_clients
EXCEPT ALL
SELECT * FROM s2_clients;

SELECT * FROM s2_clients
EXCEPT ALL
SELECT * FROM lnd_s2_clients;

--result: no errors


--s2_channels to lnd_s2_channels
select count(*) from lnd_s2_channels;
select count(*) from s2_channels;

SELECT * FROM lnd_s2_channels
EXCEPT ALL
SELECT * FROM s2_channels;

SELECT * FROM s2_channels
EXCEPT ALL
SELECT * FROM lnd_s2_channels;

--result: no errors


--s2_client_sales to lnd_s2_client_sales
select count(*) from lnd_s2_client_sales;
select count(*) from s2_client_sales;

SELECT * FROM lnd_s2_client_sales
EXCEPT ALL
SELECT * FROM s2_client_sales;

SELECT * FROM s2_client_sales
EXCEPT ALL
SELECT * FROM lnd_s2_client_sales;

select count(distinct client_id) from s2_client_sales;
select count(distinct client_id) from lnd_s2_client_sales;


SELECT client_id, channel_id, saled_at, product_id, COUNT(*) AS duplicate_count
FROM s2_client_sales
GROUP BY client_id, channel_id, saled_at, product_id
HAVING COUNT(*) > 1;


SELECT *
FROM s2_client_sales
WHERE client_id IS NULL 
   OR channel_id IS NULL 
   OR saled_at IS NULL 
   OR product_id IS NULL;

SELECT *
FROM lnd_s2_client_sales
WHERE client_id IS NULL 
   OR channel_id IS NULL 
   OR saled_at IS NULL 
   OR product_id IS NULL;


--result: error 5 rows missing


--s2_locations to lnd_s2_locations
select count(*) from lnd_s2_locations;
select count(*) from s2_locations;

SELECT * FROM lnd_s2_locations
EXCEPT ALL
SELECT * FROM s2_locations;

SELECT * FROM s2_locations
EXCEPT ALL
SELECT * FROM lnd_s2_locations;

select location_id, count(*)
from s2_locations sl 
group by location_id 
having count(*) > 1;

select location_id, count(*)
from lnd_s2_locations sl 
group by location_id 
having count(*) > 1;


--result: doubling 5 rows from source


-- I found value NA in location_src_id, but must be N/A
SELECT *
FROM dwh_locations
WHERE location_src_id != 'N/A'
  and location_src_id !~ '^[0-9]+$';

--result: 15 rows with wrong value


--Pobaluemsja s information_schema

select table_name, column_name, is_nullable, data_type
from information_schema."columns"
where table_schema = 'public'
and table_name = 'dwh_sales';



