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
--2. Testing (try to find additional bugs/issues)


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


--

select * from s2_clients;

















