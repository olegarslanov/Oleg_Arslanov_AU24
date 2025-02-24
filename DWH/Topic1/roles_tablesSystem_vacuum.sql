--Task 0 PREREQUISITE TASK

--create db
create database test_db;
--drop schema labs cascade;

create schema labs;

--create table
CREATE TABLE labs.my_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

insert into labs.my_table (id, name, created_at)
values (1, 'Oleg', current_date), (2, 'my_user', current_date), (3, 'Tatjana', current_date)
;

--select * from labs.my_table;

--create user with full access
CREATE ROLE my_user WITH LOGIN PASSWORD 'my_user';
GRANT CONNECT ON DATABASE test_db TO my_user;
grant usage on schema labs to my_user;
GRANT SELECT, INSERT ON TABLE labs.my_table TO my_user;

-- create user only with read
create role Oleg with login password 'Oleg';
GRANT CONNECT ON DATABASE test_db TO Oleg;
GRANT USAGE ON SCHEMA labs TO Oleg;
GRANT SELECT ON TABLE labs.my_table TO Oleg;


--adding row level security on table 
ALTER TABLE labs.my_table ENABLE ROW LEVEL SECURITY;

--user only read own row
CREATE POLICY person_read_own
ON labs.my_table
FOR SELECT
USING (name = current_user::text or current_user = 'postgres');


--SELECT usename 
--FROM pg_user 
--WHERE usesuper = true;



---Test for all is working

--set userwith access
set role my_user;
--set like external user without access
set role Oleg;
--set like admin all inclusive
set role postgres;

-- query to select all customers
select * from labs.my_table;


-- Task 1 -- create new db

--drop schema labs cascade;

--check about databases
select d.oid, d.datname, d.datistemplate, d.datallowconn, t.spcname
from pg_database d
join pg_tablespace t on t.oid = d.dattablespace;


--create database nyc_taxi template nyc_taxi_copy;
--create database test2 template test_db;
--SELECT datname, datacl FROM pg_database WHERE datname = 'test2';
--select current_user;


-- Task 2 --remove db in another place

--create new tablespace
create tablespace mytablespace location 'C:/Program Files/PostgreSQL/17/data/tblspc_test/';

--check what tablespaces exists
select *
from pg_tablespace; 

--move test_db to new tablespace:
ALTER DATABASE test_db SET TABLESPACE mytablespace;

--check about databases
select d.oid, d.datname, d.datistemplate, d.datallowconn, t.spcname
from pg_database d
join pg_tablespace t on t.oid = d.dattablespace;



-- Task 3 --create schema and table

--create schema
create schema labs;

--create table
CREATE TABLE labs.person (
id integer NOT NULL,
name varchar(15)
);

--check
SELECT schemaname, tablename 
FROM pg_tables
WHERE tablename = 'person';

--insert values
INSERT INTO labs.person VALUES(1, 'Bob');
INSERT INTO labs.person VALUES(2, 'Alice');
INSERT INTO labs.person VALUES(3, 'Robert');

--smotriu kakije schemu postgre ispolzuet po umolchaniju 
show search_path;


--ustnavlivaju search_path na moju schemu
set search_path to labs;

INSERT INTO person VALUES(1, 'Bob');
INSERT INTO person VALUES(2, 'Alice');
INSERT INTO person VALUES(3, 'Robert');

select *
from person;

-- Task 4

-- create extension (pozvoliaet analizirovat vnutrenniuju strukturu stranic bd)
CREATE EXTENSION pageinspect;


select 
	p.id,
	p.name,   
	p.ctid,   --fiziceskoje mestopolzenije stroki v tablice (nomer stranicy i nomer zapisi)
	p.xmin,   --indetifikator tranzakcij cto sozdala etu stroku
	p.xmax    --identifikator tranzakcij cto udalila ili obnovila stroku (esli stroka sushestvuet to 0 )
from person p;

--glubokij analiz fizicheskogo hranenija dannyh v tablice na 0 stranice tablicy
SELECT t_xmin, t_xmax, t_ctid, --t_xmin ID tranzakcij cto vstavila stroku, t_xmax ID tranzakcij cto udalila/obnovila stroku, t_ctid fiziceskij adres zapisi(nomer stranicy, nomer pozicij)
tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits) -- razbiraet hranymyje dannyje stroki v chitaemom vide
FROM heap_page_items(get_raw_page('labs.person', 0)); -- get_raw_page zagruzaet syroj binarnyj kod, a heap_page_items dekodiruet binarnyj blok v chitaeemuju tablicu


INSERT INTO person VALUES(4, 'John');

UPDATE person set name = 'Alex' where id = 2;

DELETE FROM person WHERE id = 3;

INSERT INTO person VALUES(999, 'Test');
DELETE FROM person WHERE id = 999;



-- Task 5

SELECT t_xmin, t_xmax, t_ctid,
tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

vacuum labs.person; --ochishaem tablicu ot mertvyh strok

INSERT INTO person VALUES(4, 'Kuzia2');

select *
from person
order by id;

vacuum full labs.person;









