--убрать все ограничения на нотнулл в таблице
SELECT 'alter table '||table_schema||'.'||table_name||' alter column '||column_name||' drop not null;'
FROM information_schema.columns WHERE table_name = 'courier_events_data_enriched' and table_schema='stage_nats' and is_nullable='NO';
