--- для правильного времени вставки:
       nowInBlock()                                                           AS _row_created,
       d._row_created                                                         as _raw_created
FROM shk_storage.shk_on_place_raw d
SETTINGS max_block_size = 1000000
FORMAT PrettyCompactMonoBlock;

-- Правильный парсинг координат из жсон
Обьявление: 
    latitude          Nullable(Decimal(12,9)),
    longitude         Nullable(Decimal(12,9)),
 Парсинг:
       nullIf(cast(round(toDecimal64OrNull(JSONExtract(message, 'latitude', 'Nullable(String)'),14),9) as Nullable(Decimal(12,9))),0) AS latitude,
       nullIf(cast(round(toDecimal64OrNull(JSONExtract(message, 'longitude', 'Nullable(String)'),14),9) as Nullable(Decimal(12,9))),0) AS longitude,

--эталон рав таблицы:
CREATE TABLE stage_nats.shipping_boxes_raw
(
    message    String,
    _topic     LowCardinality(String),
    _key       String,
    _offset    UInt64,
    _timestamp Nullable(DateTime),
    _partition UInt8,
    _row_created DateTime
) ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(_row_created)
        ORDER BY _key
          TTL toStartOfDay(_row_created) + INTERVAL 1 MONTH DELETE
    SETTINGS merge_with_ttl_timeout = 2400
    ;


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
select
    arrayJoin(JSONExtractArrayRaw(message)) as row
    --,arrayJoin(JSONExtractArrayRaw(row, 'shks')) AS shks
from stage_external.kafka_table_raw
     where _row_created>'2023-06-01'
limit 100000;
