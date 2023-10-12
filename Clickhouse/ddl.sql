-- создать колумн
alter table test.buf_unloaded_rids add column last_action_id Nullable(Int32);

--поменять ттл
ALTER TABLE sales_data.position_changes_ordo_raw MODIFY TTL _row_created + toIntervalMonth(3);

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

-- найти справочники, которые ссылаются на определённую таблицу с другого сервера.


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

-- индексы
alter table positions.position_changes add INDEX shk_idx(shk_id) TYPE bloom_filter GRANULARITY 3;
alter table positions.position_changes MATERIALIZE INDEX shk_idx;
