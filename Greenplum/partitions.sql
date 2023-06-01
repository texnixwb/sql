--получить список имён партиций у таблицы
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'test_' AND table_name like 'test_partitions_new%'
and table_type='BASE TABLE';

-- подмена партиции таблицей, 
ALTER TABLE test_.test_partitions
  EXCHANGE PARTITION FOR (DATE '20220101')
  WITH TABLE test_.test_partitions_new_part;
  
-- но если нужно обменяться партициями между таблицами партиционированными, то нужно импользовать промежуточную таблицу пустую, без партиций:
ALTER TABLE test_.v_late_rids_duration_in_poo
  EXCHANGE PARTITION FOR (DATE '20230501')
  WITH TABLE test_.v_late_rids_duration_in_poo_one_part;

ALTER TABLE olap.v_late_rids_duration_in_poo
  EXCHANGE PARTITION FOR (DATE '20230501')
  WITH TABLE test_.v_late_rids_duration_in_poo_one_part;
