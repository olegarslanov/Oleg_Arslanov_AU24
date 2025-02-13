-- Task 1 -- create new db

--create db
create database test_db;


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









