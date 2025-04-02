-- TASK 1

-- SELECT user, host FROM mysql.user WHERE user = 'oleg_arslanov';


-- create user oleg_arslanov with password oleg_arslanov
CREATE USER if not exists 'oleg_arslanov'@'%' IDENTIFIED BY 'oleg_arslanov';

-- grant privileges for user
GRANT ALL PRIVILEGES ON dilab_dev.* TO 'oleg_arslanov'@'%';             -- where % in MySQL means that user can connect from every IP or host and @ used for connect name with host
GRANT ALL PRIVILEGES ON oleg_arslanov_schema.* TO 'oleg_arslanov'@'%';


-- check which user connected to this session
SELECT CURRENT_USER();
-- SELECT DATABASE();




-- create schema
create schema if not exists oleg_arslanov_schema;

-- drop schema oleg_arslanov_schema;

-- Use schema for dont writing schema, only need to wrote table names only
USE oleg_arslanov_schema;

-- Drop and recreate table
-- DROP TABLE IF EXISTS test_table;
CREATE TABLE if not exists test_table (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Drop and recreate view
DROP VIEW IF EXISTS test_view;
CREATE VIEW test_view AS
SELECT * FROM test_table;

-- Drop and recreate procedure
DROP PROCEDURE IF EXISTS insert_into_test;

DELIMITER //  -- using for MySQL we declare that now end of command is '//' 
CREATE PROCEDURE insert_into_test(IN p_id INT, IN p_name VARCHAR(100))
BEGIN
    INSERT INTO test_table (id, name) VALUES (p_id, p_name);
END;
//
DELIMITER ; -- here we end using delimetr ... so ; using like end of command


-- call procedure
CALL insert_into_test(100, 'God');


-- test data
select * from test_table;
select * from test_view;


-- TASK 2

create schema if not exists dilab_dev 2.oleg_arslanov;

CREATE TABLE oleg_arslanov.my_customer AS
SELECT * FROM sakila.customer;


SELECT 
  store_id,
  COUNT(*) AS total_customers
FROM oleg_arslanov.my_customer
GROUP BY store_id;



--  Task 3




