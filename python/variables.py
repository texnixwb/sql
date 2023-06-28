# Для неиспользуемых переменных использовать имя: _
min_time, max_time, _, rec_count = get_ch_dwh_max(conn_ch, src_table_name, row_created, max_dt, max_hour_offset=max_hour_offset_d, max_batch_records=max_batch_records_d)
