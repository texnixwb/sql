 -- все процессы пользователя
 select pid,xact_start,query,usename,rsgname,state
 ,waiting_reason
 from pg_stat_activity_allusers
 --where state='active'
 order by xact_start;
