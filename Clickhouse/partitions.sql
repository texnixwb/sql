--Отключить репликацию
ALTER TABLE t1_new MODIFY SETTINGS 'replication_alter_partitions_sync' = 2;
--включить репликацию
ALTER TABLE t1 MODIFY SETTINGS 'replication_alter_partitions_sync' = 1;
