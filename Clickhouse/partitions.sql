--Отключить репликацию
ALTER TABLE t1_new MODIFY SETTINGS 'replication_alter_partitions_sync' = 2;
--включить репликацию
ALTER TABLE t1 MODIFY SETTINGS 'replication_alter_partitions_sync' = 1;

--генерация запросов для корректной очистки таблиц перед дропом:
--очищаем столбцы в партициях от данных, не включая ключ и партиции

with
    parts as
    (select distinct partition,database,table from system.parts where table='srid_tracker_prepared_v1' and database='srid_tracker')
    ,not_klear as (
select case when length(part_key)>0 then part_key||','||sorting_key else sorting_key  end as keys
    from( select
      replaceAll(replaceAll(
      replaceAll(replaceAll(replaceAll(replaceAll(partition_key,'toYYYYMMDD',''),'toYYYYMM',''),'toYear',''),'toDate','')
      ,')',''),'(','') as part_key
        , sorting_key
from system.tables where database=(select distinct database from parts)
                         and table=(select distinct table from parts)
         )
)
select sql_cript,dd.data_compressed_bytes
    from (
    select data_compressed_bytes, 'ALTER TABLE '||cc.database||'.'||cc.table||' CLEAR COLUMN '||cc.name||' IN PARTITION '||pp.partition||';' as sql_cript
from system.columns cc
join parts pp using (database,table)
    where cc.name not in (select toString(arrayJoin(splitByChar(',', assumeNotNull((select keys from not_klear))) AS src)))
    order by data_compressed_bytes desc
--теперь дропаем партиции по одной (ибо вся таблица с данными партиций и ордербаем обычно больше 50гб)
union all
select 1+row_number() over (partition by 1) as data_compressed_bytes,'ALTER TABLE '||cc.database||'.'||cc.table||' DROP PARTITION '||cc.partition||';' as sql_cript
    from parts cc
--и дроп самой таблицы
union all
select 0 as data_compressed_bytes,'drop table '||cc.database||'.'||cc.table||';' as sql_cript
    from parts cc group by cc.database,cc.table
) as dd
order by dd.data_compressed_bytes desc;
