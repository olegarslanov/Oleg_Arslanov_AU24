select d.oid, d.datname, d.datistemplate, d.datallowconn, t.spcname
from pg_database d
join pg_tablespace t on t.oid = d.dattablespace