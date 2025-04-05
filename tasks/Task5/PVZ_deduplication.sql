--create database test;


-- dumaju zdes nado nicego ne delat eti tablichki dolzhny byt v moih *.csv failah i mne nado budet ih prochitat
create table if not exists public.src1_table (
	country_id bigint primary key,
	country_name varchar
);

insert into public.src1_table (country_id, country_name)
select 10, 'BELARUS'
where not exists (
	select 1 from public.src1_table
	where upper(country_name) = upper('BELARUS')
);

insert into public.src1_table (country_id, country_name)
select 20, 'RUSSIA'
where not exists (
	select 1 from public.src1_table
	where upper(country_name) = upper('RUSSIA')
);


create table if not exists public.src2_table (
	country_id bigint primary key,
	country_name varchar
);

insert into public.src2_table (country_id, country_name)
select 1, 'Poland'
where not exists (
	select 1 from public.src2_table
	where upper(country_name) = upper('Poland')
);

insert into public.src2_table (country_id, country_name)
select 2, 'Belarus'
where not exists (
	select 1 from public.src2_table
	where upper(country_name) = upper('Belarus')
);


create table if not exists public.transaction_src1 (
	transaction_id bigint primary key,
	country_id bigint,
	price int, 
	foreign key (country_id) references public.src1_table (country_id)
);

insert into public.transaction_src1 (transaction_id, country_id, price)
select 101010, 10, 100
where not exists (
	select 1 from public.transaction_src1
	where transaction_id = 101010
);

insert into public.transaction_src1 (transaction_id, country_id, price)
select 202020, 20, 200
where not exists (
	select 1 from public.transaction_src1
	where transaction_id = 202020
);


create table if not exists public.transaction_src2 (
	transaction_id bigint primary key,
	country_id bigint,
	price int, 
	foreign key (country_id) references public.src2_table (country_id) -- Исправлена ссылка на src2_table
);

insert into public.transaction_src2 (transaction_id, country_id, price)
select 111, 1, 110
where not exists (
	select 1 from public.transaction_src2 -- Исправлена таблица проверки
	where transaction_id = 111
);

insert into public.transaction_src2 (transaction_id, country_id, price)
select 222, 2, 220
where not exists (
	select 1 from public.transaction_src2 -- Исправлена таблица проверки
	where transaction_id = 222
);

-----------------------------------------------------------------------------
--create mapping table on BL_CL 

CREATE TABLE IF NOT EXISTS public.t_map_countries (
    country_id BIGINT,
    country_name VARCHAR,            
    country_src_name VARCHAR,        
    country_src_id BIGINT,
    source_table TEXT,
    source_system TEXT,
    primary key (country_src_name, country_src_id) -- уникальность
);

INSERT INTO public.t_map_countries (country_id, country_name, country_src_name, country_src_id, source_table, source_system)
SELECT 
    DENSE_RANK() OVER (ORDER BY INITCAP(country_name)) AS country_id,
    INITCAP(country_name) AS country_name,
    pp.country_name AS country_src_name,
    pp.country_id AS country_src_id,
    pp.source_table,
    'src system' AS source_system
FROM (     
    SELECT country_id, country_name, 'src1_table' AS source_table FROM public.src1_table WHERE country_name IS NOT NULL
    UNION ALL
    SELECT country_id, country_name, 'src2_table' AS source_table FROM public.src2_table WHERE country_name IS NOT NULL
) AS pp
ON CONFLICT (country_src_name, country_src_id) DO NOTHING;

   
        
        


----------------------------------------------------------------------------
--load data into BL_3NF table

CREATE TABLE IF NOT EXISTS public.ce_countries (
    country_id BIGINT PRIMARY KEY,
    country_name VARCHAR,           
    country_src_id BIGINT,
    source_table TEXT,
    source_system TEXT
);

INSERT INTO public.ce_countries (country_id, country_name, country_src_id, source_table, source_system)
SELECT 
    COALESCE(map.country_id, src.country_id, src2.country_id) AS country_id,
    map.country_name AS country_name,
    map.country_src_id AS country_src_id,
    'bl_cl.t_map_countries' AS source_table,
    'bl_cl' AS source_system
FROM public.t_map_countries map
LEFT JOIN public.src1_table src ON src.country_id = map.country_src_id AND src.country_name = map.country_src_name
LEFT JOIN public.src2_table src2 ON src2.country_id = map.country_src_id AND src2.country_name = map.country_src_name
on conflict(country_id) do nothing;      






--truncate public.src1_table;
drop table public.t_map_countries;
drop table public.ce_countries;
truncate public.t_map_countries;
select * from public.src1_table;
select * from public.src2_table;
select * from public.transaction_src1;
select * from public.transaction_src2;
select * from public.ce_countries;
select * from public.t_map_countries;