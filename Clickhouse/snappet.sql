--преобразование текста в число:
toInt64(halfMD5(srid)) as rid

-- множественный case
                                       multiIf(wh_tare_entry = 'ERP', 1,
                                                wh_tare_entry = 'LOGISTICS', 2,
                                                wh_tare_entry = 'WBGO', 3,
                                                wh_tare_entry = 'WH_RPK', 4,
                                                wh_tare_entry = 'PVZ', 5,
                                                wh_tare_entry = 'PPVZ',6,
                                                wh_tare_entry = 'WH_TRBX', 7,
                                                wh_tare_entry = 'WBBOX', 8, 99) as wh_tare_entry_id,

--округление до часа, чтобы все записи этого часа учитывались
toStartOfHour(date_on_shelf+interval '1' HOUR) 

--проверка на перенос строки
if(position('\n' IN tare_sticker) > 0, 1, 0)

--способы вычисления наибольшего времени:
select arrayMax([now(),now()-interval '1' DAY]);
greatest
взять даты в такие конструкции reinterpretAsUInt64()
, тогда будут выводиться текущие время и дата или численное представление даты, конвертируемое обратно с помощью reinterpretAsDateTime()

SELECT toDateTime(max2(toUnixTimestamp(now()), toUnixTimestamp(now() - INTERVAL '1' DAY)))

with cte as (select now() as m1,toDateTime('2024-10-07') as m2, null as m3)
select greatest(m1,m2,m3),arrayMax([m1,m2]) ,arrayMin([m1,m2])
,arrayReduce('max', [m1,m2,m3]) -- лучшие варианты, отбрасывает нуллы а из остальных считает
,arrayReduce('min', [m1,m2,m3])
from cte;

--массовый дроп таблиц и вьюх 
select 'drop '||if(engine like '%View%','view','table')||' '||database||'.'||name||';'
from system.tables where create_table_query like '%wb_box%';

--создать табличку ас
CREATE TABLE my_data.clients
Engine = ReplacingMergeTree
ORDER BY id
AS SELECT * FROM my_data_pg.clients



-- генерируем на ч4
select replace(create_table_query,'CREATE DICTIONARY dictionaries.','CREATE DICTIONARY IF NOT EXISTS dict.')||';' as q
from system.tables where database='dictionaries' and create_table_query like '%ODBC%';

-- пересоздать все дикты при переезде на 24.8 версию
SELECT 'drop DICTIONARY dictionaries.'||table||'; '||
substring(create_table_query, 1, position(create_table_query, 'SOURCE') - 1)
||'SOURCE(CLICKHOUSE(NAME dictator_ch4 DB ''dict'' QUERY ''select * from dict.'||table||''')) LIFETIME(MIN 8640 MAX 86400) LAYOUT(HASHED_ARRAY);'
from system.tables where database='dictionaries' and create_table_query like '%ODBC%';
