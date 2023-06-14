--Дедубликация возможна только по всем ключам. включая партиции, потому, если предполагается периодическая дедубликация, нельзя создавать партиции вида:
--toYYYYMM(row_created), так как это не подставишь в OPTIMIZE TABLE, потому лучше делать отдельное поле с toUInt32(toYYYYMM(row_created)):: UInt32
--чтобы можно было внутри партиции убрать все дубли по ключу.
--дедубликация партиции:
OPTIMIZE TABLE test.position_changes PARTITION 202201 FINAL DEDUPLICATE BY srid,shk_id,dt,create_dt,price,wh_rid,status_id;
отложенная дедубликация:
OPTIMIZE TABLE positions.position_changes ON CLUSTER distributed_cluster_1 DEDUPLICATE BY srid,shk_id,dt,create_dt,price,wh_rid,status_id,payment_type,logistic_cost;
