--Посмотреть процессы:
SELECT user    ,round(read_rows / 1000 /1000) as read_rows_kk    ,round(total_rows_approx / 1000 /1000) as total_rows_kk    , round(read_rows / (total_rows_approx / 100), 2) percent_s
, round(elapsed) as elapsed , round((elapsed / percent * 100) - elapsed) left_time, round(read_bytes / 1024 /1024) read_Mgb, round(peak_memory_usage / 1024 /1024) MemoryMb  ,query,query_id 
FROM system.processes;

--подписанные:
SELECT user
     ,toString(round(read_rows / 1000 /1000))||'kk' as read_rows_kk
     ,toString(round(total_rows_approx / 1000 /1000))||'kk' as total_rows_kk
     , round(read_rows / (total_rows_approx / 100), 2) as percent
     , toString(round(elapsed))||'s' as elapsed_s
     , toString(round((elapsed / percent * 100) - elapsed))||'s' as  left_time
     , toString(round(read_rows / 1000/elapsed))||'k/s' as speed
     , toString(round(read_bytes / 1024 /1024))||'Mb' as read_Mgb
     , toString(round(read_bytes / 1024/ 1024/ elapsed))||'Mb/s' as speed_disk
     , toString(round(peak_memory_usage / 1024 /1024))||'Mb mem' as MemoryMb
     ,query,query_id
FROM system.processes;


--убить конкретный процесс
KILL QUERY WHERE query_id='c5b75dbd-fb0d-4802-9d25-06bdfc92201d';
--убить процессы одного юзера, с ожиданием завершения убийства
KILL QUERY WHERE user='superset' SYNC;


--мутации проверить и убить лишние, они не откатываются, фиксируется то что закончилось
select 'KILL MUTATION WHERE mutation_id ='''||mutation_id||''';' , * from system.mutations ;


--какие процессы:
SHOW PROCESSLIST;

--какие background-операции:
SELECT * FROM system.merges;

--Посмотреть выполняемые квери за последние пару дней:
select query from system.query_log     where query like'%postgresql%'      limit 100;
             
-- таблица справочников
select database,name,status,type,round(bytes_allocated/1024/1024) as Mb,query_count,element_count,source,comment
from system.dictionaries
order by bytes_allocated desc;

--ддл таблиц содержащих слово:
select * from system.tables         where create_table_query like '%oof-positions-final%'         limit 100;

--деаттаченые партиции:
select * from system.detached_parts;

--свободное место и всего места на дисках:
select free_space/1024/1024/1024 as  free_space_gb,total_space/1024/1024/1024 as  total_space_gb,unreserved_space/1024/1024/1024 as  unreserved_space_gb
from system.disks;

--сравнение сколько занято на диске, и сколько весят таблицы:
select round((total_space/1024/1024/1024)-(free_space/1024/1024/1024)) as fill_disk from system.disks
union all    select round(sum(bytes_size/1024/1024/1024)) as fill_disk from meta.tables_info;

--сравнение двух запросов по производительности
WITH
    query_id = '58c8958d-f571-4fe4-97a6-4b55429a1a67' AS first,
    query_id = '39cfc5ad-65b7-4f70-9451-2f1056484eb1' AS second
SELECT
    PE.Names AS metric,
    anyIf(PE.Values, first) AS v1,
    anyIf(PE.Values, second) AS v2
FROM system.query_log
ARRAY JOIN ProfileEvents AS PE
WHERE (first OR second) AND (event_date = today()) AND (type = 2)
GROUP BY metric
HAVING v1 != v2
ORDER BY
    (v2 - v1) / (v1 + v2) ASC,
    v2 ASC,
    metric ASC;

--выявление самых тормознутых квери:
select
    normalized_query_hash,
    any(query),
    round(sum(`ProfileEvents.Values`[indexOf(`ProfileEvents.Names`, 'UserTimeMicroseconds')]) / count()) userCPU,
    round(sum(`ProfileEvents.Values`[indexOf(`ProfileEvents.Names`, 'OSCPUVirtualTimeMicroseconds')]) / count()) OSCPUVirtual,
    round(sum(`ProfileEvents.Values`[indexOf(`ProfileEvents.Names`, 'OSCPUWaitMicroseconds')]) / count()) OSCPU,
    count() startCount,
    round(sum(query_duration_ms) / 1000 / count()) queryDuration,
    round(sum(read_bytes) / count() / 1024 / 1024 / 1024) readGb
from
    system.query_log
where
    type = 2 and
    event_date >= today()
group by    normalized_query_hash
having
    queryDuration > 0.5
order by
    OSCPUVirtual desc
limit 100;
