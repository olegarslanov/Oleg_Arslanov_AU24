--TASK 1: NESTED LOOP JOIN
SET search_path TO labs;


--1. Create test tables and populate them with test data:
CREATE TABLE test_joins_a
(
	id1 int,
	id2 int
);

CREATE TABLE test_joins_b
(
	id1 int,
	id2 int
);

INSERT INTO test_joins_a values(generate_series(1,10000),3);
INSERT INTO test_joins_b values(generate_series(1,10000),3);
ANALYZE; -- it is renew statistic for Query Planner, now Planner know how many rows and which values in colums


--select * from test_joins_a;

EXPLAIN ANALYZE 
SELECT * 
FROM test_joins_a a
JOIN test_joins_b b ON a.id1 = b.id1;

set enable_hashjoin = off;
set enable_mergejoin = off;
set enable_nestloop = off;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_nestloop;


EXPLAIN ANALYZE 
SELECT * 
FROM test_joins_a a
JOIN test_joins_b b ON a.id1 = b.id1;


--2. Check how NESTED LOOP JOIN method is used in below queries. Why?

set enable_hashjoin = off;
set enable_mergejoin = off;
RESET enable_nestloop;


explain analyze SELECT * 
FROM test_joins_a a, test_joins_b b
WHERE a.id1 > b.id1;

explain analyze SELECT *
FROM test_joins_a a
CROSS JOIN test_joins_b b;


--TASK 2: HASH JOIN


--Rewrite SELECT to instruct the planner to use HASH JOIN method:
--explain analyze SELECT * FROM test_joins_a a, test_joins_b b
--WHERE a.id1 > b.id1;

--set enable_nestloop= on;

explain analyze SELECT * 
FROM test_joins_a a
JOIN test_joins_b b
  ON a.id1 = b.id1
WHERE a.id1 > b.id1;


--Create query with SEMI JOIN between tables to get HASH SEMI JOIN in the plan.
EXPLAIN ANALYZE
SELECT a.*
FROM test_joins_a a
WHERE EXISTS (
    SELECT 1
    FROM test_joins_b b
    WHERE a.id1 = b.id1
);

--3. Set enable_hashjoin to off and recheck plan. Switch on enable_hashjoin.

set enable_hashjoin = off;

EXPLAIN ANALYZE
SELECT a.*
FROM test_joins_a a
WHERE EXISTS (
    SELECT 1
    FROM test_joins_b b
    WHERE a.id1 = b.id1
);

set enable_hashjoin = on;

EXPLAIN ANALYZE
SELECT a.*
FROM test_joins_a a
WHERE EXISTS (
    SELECT 1
    FROM test_joins_b b
    WHERE a.id1 = b.id1
);


--TASK 3: MERGE JOIN
--1. Using tables test_joins_a and test_joins_b create a query which is use MERGE JOIN as a join method.


SET enable_hashjoin = off;
SET enable_nestloop = off;

--create index for tables, that I hope can use then merge join with indexes
create index idx_test_joins_a_id1 on test_joins_a(id1);
create index idx_test_joins_b_id1 on test_joins_b(id1);

explain (analyze, buffers) 
select a.id1, a.id2, b.id1, b.id2
from test_joins_a a
join test_joins_b b on a.id1 = b.id1
order by a.id1;

--2. Set enable_mergejoin to off and recheck plan. Switch on enable_mergejoin.
SET enable_mergejoin = off;

explain (analyze, buffers) 
select a.id1, a.id2, b.id1, b.id2
from test_joins_a a
join test_joins_b b on a.id1 = b.id1
order by a.id1;

SET enable_mergejoin = on;

explain (analyze, buffers) 
select a.id1, a.id2, b.id1, b.id2
from test_joins_a a
join test_joins_b b on a.id1 = b.id1
order by a.id1;


--TASK 4: CHANGING JOIN ORDER

--1. Create a table and populate it with sample data:
CREATE TABLE test_joins_c
(
id1 int,
id2 int
);
INSERT INTO test_joins_c
values(generate_series(1,1000000),(random()*10)::int);

select * from test_joins_c;

--2. Check the plan. Describe the order of tables joining:
EXPLAIN
SELECT c.id2
FROM test_joins_b b
JOIN test_joins_a a on (b.id1 = a.id1)
LEFT JOIN test_joins_c c on (c.id1 = b.id1);


--3. Set join_collapse_limit = 1 and recreate plan for query above. Describe changes if any. Return join_collapse_limit = 8.

set join_collapse_limit = 1;     -- use limit that planner can not move joins for better perfomance (first join small tables)
EXPLAIN
SELECT c.id2
FROM test_joins_b b
JOIN test_joins_a a on (b.id1 = a.id1)
LEFT JOIN test_joins_c c on (c.id1 = b.id1);

set join_collapse_limit = 8;     -- use limit that planner can move joins for better perfomance up to 8 times (first join small tables)
EXPLAIN
SELECT c.id2
FROM test_joins_b b
JOIN test_joins_a a on (b.id1 = a.id1)
LEFT JOIN test_joins_c c on (c.id1 = b.id1);


--TASK 5: LATERAL JOIN

--1. Create tables and populate them by data:
CREATE TABLE orders AS
SELECT id AS order_id,
(id * 10 * random()*10)::int AS order_cost,
'order number ' || id AS order_num
FROM generate_series(1, 1000) AS id;

--drop table orders;

CREATE TABLE stores (
store_id int,
store_name text,
max_order_cost int
);

INSERT INTO stores VALUES
(1, 'grossery shop', '800'),
(2, 'bakery', '100'),
(3, 'manufactured goods', '3000')
;

--2. Create a query to find TOP 10 of orders by it cost for each store. So, on the output you should
--have 10 orders for each store (or less, depends on sample random data) with cost less than
--max_order_cost. Use LATERAL join.

select o.order_id, o.order_cost, s.store_id, s.store_name
from stores s
left join lateral (
	select o.order_id, o.order_cost 
	from orders o 
	where o.order_cost < s.max_order_cost    --with lateral subquery execute for every row outer query 
	order by o.order_cost desc
	limit 10
) o on true;


--TASK 6: RECURSIVE CTE

--1. Use emp table you created before. Select all employee and his manager name and level of
--management start from president of the company

select * from emp;

WITH RECURSIVE emp_hierarchy AS (
    -- start from president (he have mgr IS NULL)
    SELECT e.empno, e.ename, e.job, e.mgr, 1 AS level
    FROM labs.emp e
    WHERE e.job = 'PRESIDENT'  -- president first of hierarchy
    UNION all         -- recursion working with recursive + union all
    -- recursively join others employees
    SELECT e.empno, e.ename, e.job, e.mgr, eh.level + 1
    FROM labs.emp e
    JOIN emp_hierarchy eh ON e.mgr = eh.empno  -- when from emp table mgr = empo from emp_hierarchy
)
-- select all employes
SELECT eh.empno, eh.ename AS employee_name, eh.job, eh.level,
       m.ename AS manager_name
FROM emp_hierarchy eh
LEFT JOIN labs.emp m ON eh.mgr = m.empno  -- join employees with managers
ORDER BY eh.level, eh.mgr NULLS FIRST, eh.empno;


select * from emp;

--TASK 7: CHANGING DATA CTE

--1. Create log table for emp table:

CREATE TABLE order_log
(
log_id integer primary key generated always as identity,
order_id integer,
order_cost integer,
order_num text,
action_type varchar(1) CHECK (action_type IN ('U','D')),
log_date TIMESTAMPTZ DEFAULT Now()
);

select * from order_log;
select * from emp;
select * from orders;



--2. Update all rows for ORDER table:
--a. set new ORDER_COST = (old ORDER_COST / 2) where old ORDER_COST between 100 and 1000
--b. delete all rows where ORDER_COST < 50
--c. save all updated and deleted rows into log table with action type ‘U’ and ‘D’ relatively.


WITH 
updated_orders AS (
    -- update orders 
    UPDATE orders
    SET order_cost = order_cost / 2
    WHERE order_cost BETWEEN 100 AND 1000
    RETURNING order_id, order_cost, order_num, 'U' AS action_type  --returns rows for late operating
), 
deleted_orders AS (
    -- delete orders
    DELETE FROM orders
    WHERE order_cost < 50
    RETURNING order_id, order_cost, order_num, 'D' AS action_type
)
-- insert all deleted and updated orders into order_log
INSERT INTO order_log (order_id, order_cost, order_num, action_type)
SELECT order_id, order_cost, order_num, action_type FROM updated_orders
UNION ALL
SELECT order_id, order_cost, order_num, action_type FROM deleted_orders;


