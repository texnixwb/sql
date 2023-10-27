--Посмотреть процессы:
SELECT user    ,round(read_rows / 1000 /1000) as read_rows_kk    ,round(total_rows_approx / 1000 /1000) as total_rows_kk    , round(read_rows / (total_rows_approx / 100), 2) percent
, elapsed, round((elapsed / percent * 100) - elapsed, 2) left_time, round(read_bytes / 1024 /1024) read_Mgb, round(peak_memory_usage / 1024 /1024) MemoryMb  ,query,query_id 
FROM system.processes;
--убить конкретный процесс
KILL QUERY WHERE query_id='c5b75dbd-fb0d-4802-9d25-06bdfc92201d';
--убить процессы одного юзера, с ожиданием завершения убийства
KILL QUERY WHERE user='superset' SYNC;


--Посмотреть выполняемые квери за последние пару дней:
select query from system.query_log     where query like'%postgresql%'      limit 100;
             
-- таблица справочников
select database,name,status,type,round(bytes_allocated/1024/1024) as Mb,query_count,element_count,source,comment
from system.dictionaries
order by bytes_allocated desc;

--ддл таблиц содержащих слово:
select * from system.tables         where create_table_query like '%oof-positions-final%'         limit 100;

--свободное место и всего места на дисках:
select free_space/1024/1024/1024 as  free_space_gb,total_space/1024/1024/1024 as  total_space_gb,unreserved_space/1024/1024/1024 as  unreserved_space_gb
from system.disks;
