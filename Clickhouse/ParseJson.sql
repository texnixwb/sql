--- для правильного времени вставки:
       nowInBlock()                                                           AS _row_created,
       d._row_created                                                         as _raw_created
FROM shk_storage.shk_on_place_raw d
SETTINGS max_block_size = 1000000
FORMAT PrettyCompactMonoBlock;


--если arrayJoin жрёт слишком много памяти (а можно и во всех случаях так делать) стоит сделать бесконечный лимит, и всё работает моментально источник https://github.com/ClickHouse/ClickHouse/issues/10590:
limit 10000000

-- Правильный парсинг координат из жсон
Обьявление: 
    latitude          Nullable(Decimal(12,9)),
    longitude         Nullable(Decimal(12,9)),
 Парсинг:
       nullIf(cast(round(toDecimal64OrNull(JSONExtract(message, 'latitude', 'Nullable(String)'),14),9) as Nullable(Decimal(12,9))),0) AS latitude,
       nullIf(cast(round(toDecimal64OrNull(JSONExtract(message, 'longitude', 'Nullable(String)'),14),9) as Nullable(Decimal(12,9))),0) AS longitude,
--до 22.11 клика вот так надо парсить длинные текстовые суммы  '{"summ":"30.590000000000003"}':
       toDecimal64(coalesce(nullIf(JSONExtract(message, 'summ', 'Nullable(String)'),''),'0'),2) as amount
       

--эталон рав таблицы:
CREATE TABLE raw.wh_assembled_raw
(
    `message` String CODEC(ZSTD(1)),
    `_topic` LowCardinality(String),
    `_key` String,
    `_offset` UInt64 CODEC(T64, ZSTD(1)),
    `_timestamp` Nullable(DateTime),
    `_partition` UInt8,
    `_row_created` DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(_row_created)
ORDER BY _key
TTL toStartOfDay(_row_created) + toIntervalMonth(3)
SETTINGS index_granularity = 16386, merge_with_ttl_timeout = 86400, ttl_only_drop_parts = 1, index_granularity_bytes = 4194304
COMMENT 'Сборка на складе из wh-assembled'


-- получение названий полей внутри жсона с их типами:
SET allow_experimental_object_type = 1;
CREATE TABLE test.test_raw
(
    `raw` JSON
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;
truncate table test.test_raw;
insert into test.test_raw (raw)
select message
    --arrayJoin(JSONExtractArrayRaw(message)) as row
    --,arrayJoin(JSONExtractArrayRaw(row, 'shks')) AS shks
from stage_external.kafka_table_raw
     where _row_created>'2023-06-01'
limit 100000;

SET describe_extend_object_types=1;
DESCRIBE test.test_raw;

--новый вид жсона с 24.8 версии
set allow_experimental_json_type = 1;
--потому с 24.8:
SET allow_experimental_object_type = 1;
CREATE TABLE raw.describe_raw
(
    raw Object('json')
)
ENGINE = MergeTree
ORDER BY tuple();

insert into raw.describe_raw (raw)
SELECT message from raw.waysheets_close_raw limit 100000;

SET describe_extend_object_types=1;
DESCRIBE raw.describe_raw;



--Выцепить один элемент с заголовком из жсона:
select '{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}' as s,
      toJSONString(map(((arrayFilter(x -> x.1 = 'a', JSONExtractKeysAndValuesRaw(s)) as a).1)[1], arrayMap(x->toFloat64(x), JSONExtractArrayRaw((a.2)[1])))) as r;
--результат:  {"a":[-100,200]}  

select '{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}' as s,
       JSONExtract(s, 'Tuple(a Array(Float32))') as dd,
       toJSONString( JSONExtract(s, 'Tuple(a Array(Float32))') ) as c
       , toJSONString(dd) as ad
        ;


--Другим способом собрать некоторые поля жсона в отдельный жсон:
     select '"colorIDs":'||ifNull(nullIf(JSONExtractRaw(message,'colorIDs'),''),'null') as colorIDs
,'"colorParentIDs":'||ifNull(nullIf(JSONExtractRaw(message,'colorParentIDs'),''),'null') as colorParentIDs
,'"fullNmsImt":'||ifNull(nullIf(JSONExtractRaw(message,'fullNmsImt'),''),'null') as fullNmsImt
,'"nameFormula":'||ifNull(nullIf(JSONExtractRaw(message,'nameFormula'),''),'null') as nameFormula
,'"dimensions":'||ifNull(nullIf(JSONExtractRaw(message,'dimensions'),''),'null') as dimensions
,'{'||colorIDs||','||colorParentIDs||','||fullNmsImt||','||nameFormula||','||dimensions||'}' as ext_cards
from _raw;

--Битовые поля
select 16::Int64 as ff --число
     , bitOr(ff, toUInt8(power(2, 2))) as set_f --поставить бит2
     , bitAnd(set_f, bitNot(toUInt8(power(2, 2)))) as del_f --убрать бит2
     , bitTest(set_f, 2) as is_logist-- проверить 2й бит
     , bitTest(del_f, 2) as is_logist2
     , bitTest(ff, 2) as is_logist3
,arrayMap(
        i -> (bitAnd(ff, bitShiftLeft(1, i)) > 0 ? 1 : 0),
        range(64)
    ) AS bits_array_ff --вывести на экран биты
,arrayMap(
        i -> (bitAnd(set_f, bitShiftLeft(1, i)) > 0 ? 1 : 0),
        range(64)
    ) AS bits_array_set_f
,arrayMap(
        i -> (bitAnd(del_f, bitShiftLeft(1, i)) > 0 ? 1 : 0),
        range(64)
    ) AS bits_array_del_f

Документация по новому JSON типу: https://clickhouse.com/docs/sql-reference/data-types/newjson
