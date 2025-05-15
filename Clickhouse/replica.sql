--Можно для всех таблиц использовать: ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}-{shard}/{database}/{table}', '{replica}')
--Тогда будет подставляться имя текущее таблицы в зукипер, проверить можно:
 SELECT
    database,
    table,
    replica_name,
    zookeeper_path
FROM system.replicas
WHERE database = 'datamart' AND table = 'crossborder_srids_and_actions_ts';
