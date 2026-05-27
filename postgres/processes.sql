CREATE OR REPLACE FUNCTION public.pg_stat_activity_full()
RETURNS SETOF pg_stat_activity
LANGUAGE sql
SECURITY DEFINER  -- Функция выполнится с правами создавшего её суперпользователя / чтобы видеть все процессы не будучи суперсюзером
AS $$
    SELECT * FROM pg_stat_activity;
$$;
GRANT EXECUTE ON FUNCTION public.pg_stat_activity_full() TO izotov;
