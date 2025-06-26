set max_memory_usage = 60000000000; -- увеличение использования оперативы до 60гб
SETTINGS max_memory_usage = '160Gi'; -- как сеттингс при запросе
-- инсерт в много потоков, стоит использовать, если нет группировки
SETTINGS max_insert_threads=20

SET max_execution_speed = 1000; -- Не больше 1000 строк/сек ограничиваем для медленной работы

SETTINGS min_execution_speed = 5000, -- если становится слишком медленно, то дропаем запрос, проверяя каждые 10 сек скорость
             timeout_before_checking_execution_speed = 10
    
SETTINGS 
    max_execution_time = 30,           -- 30 секунд максимум
    timeout_before_checking_execution_speed = 10;  -- Проверка скорости через 10 сек
--удаление больших таблиц
set max_table_size_to_drop='100Ti';
set max_partition_size_to_drop='100Ti';
