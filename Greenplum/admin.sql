SELECT * FROM gp_toolkit.gp_resgroup_config;
SELECT * FROM gp_toolkit.gp_resgroup_status;
SELECT * FROM gp_toolkit.gp_resgroup_status_per_host;

--юзер рв и группа юзеров
create user test_rw with password 'test_rwtest_rw' connection limit 4;
grant rw to test_rw;

with cte as (select distinct schemaname
             from pg_stat_user_tables)
select 'grant usage on schema '||schemaname||' to rw;'
,'grant select,insert on all tables in schema '||schemaname||' to rw;'
,'grant select on all sequences in schema '||schemaname||' to rw;'
from cte
;

create schema test_;
alter schema test_ owner to test_rw;

--удаление юзера
drop schema test_abishev cascade;
revoke ro  from abishev;
revoke all on ALL TABLES IN SCHEMA shk_tracker from abishev;
drop user abishev;
