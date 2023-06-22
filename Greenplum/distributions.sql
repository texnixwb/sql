--получаем айди сегмента: 
select gp_segment_id,count()
from test.table_name
group by gp_segment_id
  
--получаем имя сервера по айди сегмента:
select * from gp_segment_configuration;

--размер таблицы
select * from pg_size_pretty(pg_total_relation_size('test_sh.tet_table_prt_1'));

--размер вместе с партициями
select parent, 
    pg_size_pretty(sum(total)) 
from (
    select inhparent::regclass::text as parent
            , (pg_total_relation_size(inhrelid)) total
    from (
            select *, pi.inhparent::regclass::text as name from pg_inherits pi
            ) pi
    where pi.name like '%test%' and pi.name not like '%test_sh.test_old%'
) q
GROUP BY parent;

--- неиспользуемые таблицы
SELECT
        schemaname,
        relname,
        pg_size_pretty(pg_relation_size(schemaname ||'.'|| relname)) as RelationSize
    FROM
        pg_stat_all_tables
    WHERE
        schemaname NOT IN ('pg_catalog', 'pg_toast', 'information_schema') AND        seq_scan + idx_scan = 0
    ORDER BY
        pg_relation_size(schemaname ||'.'|| relname) DESC;
