-- preparation

CREATE DATABASE DQ;

CREATE TABLE bank (
    age INTEGER,
    job VARCHAR(50),
    marital VARCHAR(20),
    education VARCHAR(30),
    "default" VARCHAR(5),
    balance INTEGER,
    housing VARCHAR(5),
    loan VARCHAR(5),
    contact VARCHAR(20),
    duration INTEGER
);

--drop table bank;


--Defects detection (not all ... dont have time)

--Anomaly 1: Unexpected Values in job Column

--defects
select job, count(*) as count 
from bank
where job = 'unknown' or job is null
group by job;


--Anomaly 2: Outliers in age Column

-- defects (we need remove those rows)
SELECT * FROM bank WHERE age > 90 or age <16;


--Anomaly 3: Mismatched Contact/Duration

SELECT age, job, contact, duration 
FROM bank 
WHERE 
	(contact = 'unknown' and duration > 0)
	or
	(contact = 'unknown' and duration = 0)
	or
	(contact != 'unknown' and duration = 0);



