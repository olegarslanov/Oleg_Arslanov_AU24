-- SCRUM-1595 Bug update (by Mentor comments)

SELECT 
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'  
  AND table_name = 'dm_main_dashboard'
ORDER BY ordinal_position;




-- SCRUM-1609 Bug update(by Mentor comments)

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

--next I found that rows 
WITH dwh_calc AS (
    SELECT 
        dcl.first_name,
        dcl.last_name,
        dcl.email,
        dch.channel_name,
        p.product_name,
        ROUND(SUM(s.quantity * p.product_cost), 2) AS total_cost
    FROM dwh_sales s
    JOIN dwh_products p ON s.product_id = p.product_id
    JOIN dwh_clients dcl ON s.client_id = dcl.client_id
    JOIN dwh_channels dch ON s.channel_id = dch.channel_id
    GROUP BY dcl.first_name, dcl.last_name, dcl.email, dch.channel_name, p.product_name
),
dm_calc AS (
    SELECT 
        client_first_name AS first_name,
        client_iast_name AS last_name,
        email,
        channel_name,
        product_name,
        ROUND(SUM(total_cost), 2) AS total_cost
    FROM dm_main_dashboard
    GROUP BY client_first_name, client_iast_name, email, channel_name, product_name
)
SELECT 
    COALESCE(dc.first_name, dmd.first_name) AS first_name,
    COALESCE(dc.last_name, dmd.last_name) AS last_name,
    COALESCE(dc.email, dmd.email) AS email,
    COALESCE(dc.channel_name, dmd.channel_name) AS channel_name,
    COALESCE(dc.product_name, dmd.product_name) AS product_name,
    COALESCE(dc.total_cost, 0) AS dwh_cost,
    COALESCE(dmd.total_cost, 0) AS dm_cost,
    ROUND(COALESCE(dc.total_cost, 0) - COALESCE(dmd.total_cost, 0), 2) AS difference_cost
FROM dwh_calc dc
FULL OUTER JOIN dm_calc dmd
    ON dc.first_name = dmd.first_name
   AND dc.last_name = dmd.last_name
   AND dc.email = dmd.email
   AND dc.channel_name = dmd.channel_name
   AND dc.product_name = dmd.product_name
WHERE ROUND(COALESCE(dc.total_cost, 0) - COALESCE(dmd.total_cost, 0), 2) != 0;


--result : we found 142 rows that DWH layer total and DM layer total sales is different



-- SCRUM-1781 Bug update(by Mentor comments)

select client_id, count(*) as cnt from lnd_s1_clients
group by client_id
having count(*) > 1;

--result: error 11 rows too much in landing source 
