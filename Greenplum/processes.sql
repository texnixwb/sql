 -- все процессы пользователя
 select pid,xact_start,query,usename,rsgname,state
 ,waiting_reason
 from pg_stat_activity_allusers
 --where state='active'
 order by xact_start;

--Отменить процесс:
select pg_cancel_backend();
-уничтожить процесс, с потерей данных о транзакции:
select pg_terminate_backend ();
--блокировки
select
  bgl.relation::regclass,  bda.pid as blocked_pid,  bda.query as blocked_query,  bdl.mode as blocked_mode,  bga.pid AS blocking_pid,  bga.query as blocking_query,  bgl.mode as blocking_mode
from pg_catalog.pg_locks bdl
  join pg_stat_activity bda    on bda.pid = bdl.pid
  join pg_catalog.pg_locks bgl    on bdl.pid != bgl.pid    and bgl.relation = bdl.relation    and bgl.locktype = bdl.locktype
  join pg_stat_activity bga    on bga.pid = bgl.pid
where not bdl.granted;

проверить статистику
SELECT * FROM gp_toolkit.gp_stats_missing
where smitable like '%test%'
and smitable not like 'test_no_need%'
