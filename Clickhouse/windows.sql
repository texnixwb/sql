-- выборка последнего по дате склада исключая нуллы
argMax(src_office_id,dwh_date) as ma_src

-- выборка последнего по дате склада с условиями массива выборки
,argMaxIf(src_office_id,dwh_date, dwh_date is not null) as ma_src
