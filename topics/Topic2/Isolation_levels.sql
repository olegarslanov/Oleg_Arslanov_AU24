
-- first transaction
begin;
SET TRANSACTION ISOLATION LEVEL serializable;
SET debug_parallel_query = off;
show transaction isolation level;
SHOW debug_parallel_query;

select *, xmin, xmax, cmin, cmax
from public.employee e;

insert into public.employee ("name", status)
values ('Bob', 'Not fired');

update public.employee set status = 'Sup New 100' 
where id=2;

commit;
rollback;

-- second transaction
begin;
show transaction isolation level;

select txid_current();

delete from public.employee
where id = 1;

select *, xmin, xmax, cmin, cmax
from public.employee e;

commit;


-- third transaction
begin;
show transaction isolation level;

select txid_current();

update public.employee
set status = 'Fired'
where id = 2;

select *, xmin, xmax, cmin, cmax
from public.employee e;

commit;