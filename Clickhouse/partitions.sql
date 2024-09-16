--Отключить репликацию
ALTER TABLE t1_new MODIFY SETTINGS 'replication_alter_partitions_sync' = 2;
--включить репликацию
ALTER TABLE t1 MODIFY SETTINGS 'replication_alter_partitions_sync' = 1;

--удаляем таблицы любого размера
set max_table_size_to_drop=5000000000000;
--генерация запросов для корректной очистки таблиц перед дропом:
--очищаем столбцы в партициях от данных, не включая ключ и партиции

--делаем это по партициям, начиная с самой старой
with
    parts as
    (select distinct partition,database,table from system.parts where table='position_changes' and database='positions')
    ,not_klear as (
select replaceAll(case when length(part_key)>0 then part_key||','||sorting_key else sorting_key  end,' ','') as keys
    from( select
      replaceAll(replaceAll(
      replaceAll(replaceAll(replaceAll(replaceAll(partition_key,'toYYYYMMDD',''),'toYYYYMM',''),'toYear',''),'toDate','')
      ,')',''),'(','') as part_key
        , sorting_key
from system.tables where database=(select distinct database from parts)
                         and table=(select distinct table from parts)         ))
select sql_cript,sort_id,partition
    from (
    --*вырезаем если не обязательно чистить колумны*
    select toInt64(pp.partition||toString(round(toInt64(data_compressed_bytes)/10000))) as sort_id
         , 'ALTER TABLE '||cc.database||'.'||cc.table||' CLEAR COLUMN '||cc.name||' IN PARTITION '||pp.partition||';' as sql_cript
    ,pp.partition
from system.columns cc
join parts pp using (database,table)
    where cc.name not in (select toString(arrayJoin(splitByChar(',', assumeNotNull((select keys from not_klear))) AS src)))
    order by data_compressed_bytes desc
--теперь дропаем партиции по одной (ибо вся таблица с данными партиций и ордербаем обычно больше 50гб)
union all
    --*конец вырезки если не обязательно чистить колумны*
select -10-row_number() over (partition by 1) as sort_id,'ALTER TABLE '||cc.database||'.'||cc.table||' DROP PARTITION '||cc.partition||';' as sql_cript
    ,cc.partition
    from parts cc
--и дроп самой таблицы
union all
select -1000 as sort_id,'drop table '||cc.database||'.'||cc.table||';' as sql_cript
    ,'0' as partition
    from parts cc group by cc.database,cc.table
) as dd
order by partition,dd.sort_id desc;
