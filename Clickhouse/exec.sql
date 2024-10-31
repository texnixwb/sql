set max_memory_usage = 60000000000; -- увеличение использования оперативы до 60гб
SETTINGS max_memory_usage = '160Gi'; -- как сеттингс при запросе
-- инсерт в много потоков, стоит использовать, если нет группировки
SETTINGS max_insert_threads=20
--удаление больших таблиц
set max_table_size_to_drop=5000000000000;
