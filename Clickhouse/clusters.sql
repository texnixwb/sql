--взять значения макросов
select getMacro('shard');
select getMacro('replica');

-- для разных {replica} синхронизируются таблицы с одинковыми путями, а чтобы разбить их на шарды, используется {shard}
ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/positions/positions_last_state', '{replica}', _ver)
