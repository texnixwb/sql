-- создать колумн
alter table test.buf_unloaded_rids add column last_action_id Nullable(Int32);

--сменить местоположение колумна
alter table positions.oof_position_changes ALTER column sm_id TYPE Nullable(Int8) AFTER shk_id;

--алгоритм применения кодеков:
--на жсоны CODEC(ZSTD(1))
--на возрастающие даты и 32,64 цифры /для редко поднимаемых данных/ CODEC(DoubleDelta, ZSTD(1)) /для частых / CODEC(Delta, ZSTD(1))
--на рандом цифры 32 и 64 CODEC(T64, ZSTD(1))
-- для флоат32 64 CODEC(FPC, ZSTD(1))

применить к новым данным:
ALTER TABLE test_table MODIFY COLUMN column_a CODEC(ZSTD(2)); 
--но работает только с новыми данными в таблицу, чтобы применить кодек к старым данным:
ALTER TABLE test_table UPDATE column_a = column_a WHERE 1

--поменять ттл
ALTER TABLE datamart.positions_on_shelf MODIFY TTL toStartOfDay(date_on_shelf) + toIntervalMonth(1);

--правильные сетинсы для ТТЛ
ALTER TABLE stage_wh.wh_sorted_raw MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE stage_wh.wh_sorted_raw MODIFY SETTING merge_with_ttl_timeout = 86400;

--удалить
alter table test.unloaded_rids  delete where dwh<now()-interval 1 DAY;

--СПРАВОЧНИКИ

CREATE DICTIONARY dictionaries.action_list
(

    action_id UInt64,
    action_description String
)
PRIMARY KEY action_id
SOURCE(ODBC(DB 'dwh' TABLE 'shk_tracker.action_list' CONNECTION_STRING 'DSN=Greenplum'))
LIFETIME(MIN 86400 MAX 86400)
LAYOUT(HASHED(PREALLOCATE 0));

--на кластере, кешированный, который занимает 500мб в памяти кеша
drop DICTIONARY dictionaries.wh_storage_places on cluster distributed_cluster_1;
CREATE DICTIONARY dictionaries.wh_storage_places on cluster distributed_cluster_1
(
    `place_id` Int32,
    `office_id` Int32,
    `place_name` String,
    `place_type_id` Int16,
    `wh_id` Int16,
     stage Int32,
     storage_id Int16,
    `is_deleted` bool
)
PRIMARY KEY place_id
SOURCE(ODBC(DB 'dwh' TABLE 'dict.wh_storage_places' CONNECTION_STRING 'DSN=Greenplum'))
LIFETIME(MIN 86000 MAX 86400)
LAYOUT(CACHE(SIZE_IN_CELLS 10000000))
COMMENT 'СЛоварь мест хранения и их параметров, большой, потому хранит в кеше то что находит в гринпламе';

-- перезагрузить все справочники
SYSTEM RELOAD DICTIONARY <dict_name>

-- по словарям табличка 
system.dictionaries

-- стандарт чтения из стрима:
CREATE TABLE streams.transactions
(    `message` String)
ENGINE = Kafka(dataops_kafka_gold)
SETTINGS kafka_topic_list = 'topic_list_name',
  kafka_group_name = 'nameserver_nametopic_group',
  kafka_format = 'JSONAsString',
  kafka_max_block_size = 100000,  
  kafka_num_consumers = 3;

CREATE TABLE stage_bo.transactions_raw (
    message    String,
    _topic     LowCardinality(String),
    _key       String,
    _offset    UInt64,
   _timestamp Nullable(DateTime),
    _partition UInt8,
   _row_created DateTime
) ENGINE = MergeTree  PARTITION BY toYYYYMMDD(_row_created)  ORDER BY _key
    TTL toStartOfDay(_row_created) + INTERVAL 3 MONTH DELETE
    SETTINGS index_granularity=16386, merge_with_ttl_timeout = 86400,ttl_only_drop_parts = 1, index_granularity_bytes=4194304
    COMMENT '<номер задачи> <описание> из <имя топика кафки>';

--стандарт хранения в архиве
CREATE TABLE stage_wh.wh_sorted_raw
(
    `message` String CODEC(ZSTD(1)),
    `_topic` LowCardinality(String) CODEC(ZSTD(1)),
    `_key` String CODEC(ZSTD(1)),
    `_offset` UInt64 CODEC(T64, ZSTD(1)),
    `_timestamp` DateTime CODEC(ZSTD(1)),
    `_partition` UInt8 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(_timestamp)
ORDER BY _key
TTL toStartOfMonth(_timestamp) + INTERVAL 24 MONTH
SETTINGS index_granularity=16386,merge_with_ttl_timeout = 2000000,ttl_only_drop_parts = 1, index_granularity_bytes=4194304
COMMENT '';


--оптимальное хранение коротких по времени архивов:
  date_bak Date comment 'Дата бекапа' 
      ----------
        PARTITION BY date_bak
        ORDER BY srid
        TTL date_bak + toIntervalMonth(1) DELETE
            , date_bak + toIntervalDay(1) RECOMPRESS CODEC(ZSTD(1))
        SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 86400, index_granularity = 32768,merge_with_recompression_ttl_timeout = 86400
      ;



-- индексы
alter table positions.position_changes add INDEX shk_idx(shk_id) TYPE bloom_filter GRANULARITY 3;
alter table positions.position_changes MATERIALIZE INDEX shk_idx;
