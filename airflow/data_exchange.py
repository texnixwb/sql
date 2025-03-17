import logging as log
from airflow.hooks.dbapi import DbApiHook

from os.path import exists as file_exists, getsize as file_size
from hooks.clickhouse_hook import ClickhouseHook
from hooks.bash_hook import BashHook
from airflow.exceptions import AirflowException
from datetime import datetime
import time
import os
from tempfile import TemporaryDirectory
from psycopg2.extensions import connection


def try_conn(connect_id: str, retry_cnt: int = 5, wait: int = 10) -> connection:
    """Попытки подключения к , в случае если свободные слоты заняты, с ожиданием

    :param connect_id: conn_id
    :type connect_id: str
    :type retry_cnt: int
    :type wait: int
    :return: connection
    :rtype: `psycopg2.extensions.connection`
    """
    #gp_hook = PostgresHook(postgres_conn_id=connect_id)
    gp_hook = DbApiHook.get_hook(connect_id)

    for i in range(retry_cnt):
        try:
            conn = gp_hook.get_conn()
        except Exception:
            log.error('[%s] попытка %s, ошибка:', connect_id, i, exc_info=True)
            time.sleep(wait)
            continue
        return conn

    log.critical('[%s] все попытки подключения исчерпались', connect_id)

    raise ConnectionRefusedError(connect_id)


def load_from_kh_to_gp_once(recipient_table: str,  # таблица назначения, например stage_bo.transaction_transaction
                            take_data: str,  # скрипт забора из кликхауза, без всяких отсечек, целиковый
                            columns: str,  # список полей в которые производится вставка shk_id,place_cod,box_id
                            connect_kh: str = 'do-ch0',  # коннект к истоничку-клику
                            connect_gp: str = 'do-greenplum',  # коннект к гринпламу
                            buffer_table: str = '',  # буферная таблица, для delete/insert
                            delete_script: str = '',  # скрипт очистки таблицы назначения
                            replace_insert_script: str = ''
                            # скрипт выполняемый прямо перед переброской из буфера в назначение
                            ):
    """функция забирающая данные из клика FORMAT CSV; в csv и закидывающая их в гп.(только для гп, так как 9.4 постгрес)
      все данные из скрипта целиком в одном файле
    """
    with TemporaryDirectory(prefix='dataex') as tmp_dir:
        csv_file = f'{tmp_dir}/{recipient_table}.csv.gz'

        ch_bash = BashHook(connect_kh)
        ch_connect = ' clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
                     ' --password="{password}" --query="{take_data}" | gzip > {csv_file}'
        gp_bash = BashHook(connect_gp)
        gp_connect = f'zcat {csv_file}' + '| PGPASSWORD="{password}" psql -h {host} -p {port} -U {login} -d {schema} ' \
                                          '-v ON_ERROR_STOP=1 -c "{load_to_gp}"'

        load_to_gp = f"\copy {buffer_table if buffer_table else recipient_table} ({columns}) " \
                     f"from STDIN with (format csv , null '\\N')"

        gp_command = gp_bash.render_command(gp_connect, load_to_gp=load_to_gp)
        try:
            ch_command = ch_bash.render_command(ch_connect, take_data=take_data, csv_file=csv_file)
            ch_bash.run(ch_command, displayed_bash_command='Export to csv')
        except Exception as e:
            log.info(e)
            raise AirflowException(f'Не вышло забрать данные ') from None
        # проверка сформированного файла
        if file_exists(csv_file) and file_size(csv_file) > 20:  # 20 bytes empty gzipped data
            log.info(f'found file with size {file_size(csv_file)}')

            count_rows_csv = gp_bash.run(f'zgrep -c ^ {csv_file}', return_stdout=True)
            count_rows_csv = int(count_rows_csv)
        else:
            count_rows_csv = 0
        log.info(f'count rows in file {csv_file} = {count_rows_csv}')

        if (int(count_rows_csv) if count_rows_csv else 0) == 0:
            os.remove(f'{csv_file}')
            log.info('No Data')
        else:
            try:
                if buffer_table:
                    with try_conn(connect_gp) as conn:
                        cursor = conn.cursor()
                        log.info(f'truncate table {buffer_table};')
                        cursor.execute(f'''truncate table {buffer_table};''')
                gp_bash.run(gp_command, displayed_bash_command="Load from csv")
                log.info('Done!')
                with try_conn(connect_gp) as conn:
                    cursor = conn.cursor()
                    if delete_script:  # скрипт выполняющийся для очистки данных перед вставкой
                        log.info(delete_script)
                        cursor.execute(delete_script)
                    if replace_insert_script:  # этот скрипт используется ВМЕСТО стандартного скрипта вставки/можно даже апдейтить
                        log.info(replace_insert_script)
                        cursor.execute(replace_insert_script)
                    else:
                        if buffer_table:
                            log.info(f'insert new rows in {recipient_table};')
                            cursor.execute(f"""
                                            insert into {recipient_table} ({columns})
                                            select {columns}   from {buffer_table} ;
                                            """)
                    log.info(f'Done {recipient_table}')
                    os.remove(f'{csv_file}')
            except Exception as e:
                log.info(e)
                os.remove(f'{csv_file}')
                raise AirflowException('process_data() failed with exception error %s' % e.__repr__()) from None


def load_from_kh_to_gp(recipient_table: str,  # таблица назначения, например stage_bo.transaction_transaction
                       take_data: str,  # скрипт забора из кликхауза, обязательно включающий  >={rv},<{po},FORMAT CSV
                       take_max: str,  # скрипт взятия максимальной даты из клика, возвращает int32
                       columns: str,  # список полей в которые производится вставка shk_id,place_cod,box_id
                       shift_rv: int = 20000,  # сдвиг отсечки в секундах все отсечки - это датавремя в секундах.
                       connect_kh: str = 'clickhouse_v3_native',  # 'clickhouse_v3_native'
                       connect_gp: str = 'postgres_greenplum',  # 'postgres_greenplum'
                       buffer_table: str = '',  # буферная таблица, для delete/insert
                       delete_script: str = '',  # скрипт очистки таблицы назначения
                       replace_insert_script: str = '',
                       # скрипт выполняемый прямо перед переброской из буфера в назначение
                       take_all: str = '',
                       # если забираем всё (может буферка на клике), то all, если из обновляющейся, то смещаем на 10
                       on_conflict: int = 0,  # 1 если используем ON CONFLICT DO NOTHING (>9.4)
                       debug: int = 0,  # Если дебаг, используем определённый test_izotov схему
                       time_cut: int = 20,  # смещение относительно отсечки po
                       while_cnt: int = 10,  # количество файлов, которые надо обработать в рамках одного цикла.
                       cutoff_name: str = None # имя отсечки, если имя <recipient_table> занято или не подходит
                       ):
    """функция забирающая данные из клика FORMAT CSV; в csv и закидывающая их в гп.(только для гп, так как 9.4 постгрес)
     Обязательно в скрипте забора меньше отсечки "по"  < {po}
    """
    if debug == 1:
        connect_gp = 'postgres_greenplum_online'
    """пример"""
    # load_from_kh_to_gp('stage_bo.transaction_transaction',
    #                   """select transaction_id, transaction_uid, transaction_dt, create_dt, create_employee_id
    #                           , office_id, wbuser_id, ifNull(currency_id,810) as currency_id, wd_shift_id, source_doc_type_id, dwh_date
    #                           from stage_bo.transaction_transaction
    #                           where dwh_date >= {rv} and dwh_date < {po}
    #                           and dwh_date>=toDateTime('2021-12-17 00:00:01')
    #                           FORMAT CSV ;""",
    #                   """select toInt32(max(dwh_date)) from stage_bo.transaction_transaction;""",
    #                   """transaction_id, transaction_uid, transaction_dt, create_dt, create_employee_id
    #                        , office_id, wbuser_id, currency_id, wd_shift_id, source_doc_type_id, dwh_date""",
    #                   20000,
    #                   'clickhouse_v3_native',
    #                   'postgres_greenplum'
    #                   )
    with TemporaryDirectory(prefix='dataex') as tmp_dir:
        csv_file = f'{tmp_dir}/{recipient_table}.csv.gz'

        ch_bash = BashHook(connect_kh)
        ch_hook = ClickhouseHook(connect_kh)
        ch_connect = ' clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
                     ' --password="{password}" --query="{take_data}" | gzip > {csv_file}'
        gp_bash = BashHook(connect_gp)
        gp_connect = f'zcat {csv_file}' + '| PGPASSWORD="{password}" psql -h {host} -p {port} -U {login} -d {schema} ' \
                                          '-v ON_ERROR_STOP=1 -c "{load_to_gp}"'

        load_to_gp = f"\copy {buffer_table if buffer_table else recipient_table} ({columns}) " \
                     f"from STDIN with (format csv , null '\\N')"

        gp_command = gp_bash.render_command(gp_connect, load_to_gp=load_to_gp)
        with try_conn(connect_gp) as conn:
            cutoff_name = cutoff_name or recipient_table
            cursor = conn.cursor()
            log.info('Took max PG')
            if debug == 0:
                cursor.execute(f'''select max_int from last_max_val_cutoff where table_name='{cutoff_name}' ''')
            else:
                cursor.execute(
                    f'''select max(max_int) from test_izotov.max_val_cutoff where table_name='{cutoff_name}' ''')
            try:
                created_cutoff = cursor.fetchone()[0]
            except TypeError:
                log.critical('The cutoff was not found. Is it really in the table?')
                raise
            # toInt32(toDateTime('2021-01-01 01:00:01')) = 1609452001
            if created_cutoff is None:
                with ch_hook.get_conn() as kh_conn:
                    kh_cursor = kh_conn.cursor()
                    kh_cursor.execute(take_max.replace('max(', 'min('))
                    created_cutoff = kh_cursor.fetchone()[0]
        with ch_hook.get_conn() as conn:
            cursor = conn.cursor()
            log.info('Took max KH')
            cursor.execute(take_max)
            if take_all:
                end_cutoff = cursor.fetchone()[
                                 0] + 1  # если есть значение в take_all, значит высасываем всё что там есть.
            else:
                end_cutoff = cursor.fetchone()[0] - time_cut  # смещаем на 20 секунд, чтобы не терять ещё вставляющиеся.
        if end_cutoff < created_cutoff:
            return 'Нет данных в таблице-источнике'
        current_cutoff = created_cutoff
        if current_cutoff + shift_rv <= end_cutoff:
            po = current_cutoff + shift_rv
        else:
            po = end_cutoff
        i = 0
        while (po <= end_cutoff) and (i < while_cnt):
            i += 1
            log.info(f'Файл №{i} из {while_cnt}')
            log.info(f'Отсечка с {datetime.utcfromtimestamp(current_cutoff)} по {datetime.utcfromtimestamp(po)}')
            # часта ошибка с переполнением буфера клика, потому пробуем уменьшать отсечку для уменьшения потребления памяти.
            try_push = 0
            while shift_rv > 10 and try_push == 0:
                try:
                    ch_command = ch_bash.render_command(ch_connect, take_data=take_data.format(rv=current_cutoff, po=po)
                                                        , csv_file=csv_file)
                    ch_bash.run(ch_command, displayed_bash_command='Export to csv')
                    try_push = 1
                except Exception as e:
                    log.info(e)
                    shift_rv = round(shift_rv / 2)
                    if shift_rv < 21 and str(e) == 'DB::Exception':
                        raise AirflowException('Ошибка выполнения скрипта забора \n' + take_data.format(rv=current_cutoff, po=po))
                    log.info(f'Пробуем сдвиг {shift_rv}')
                    po = current_cutoff + shift_rv

            if shift_rv <= 2:
                raise AirflowException(f'Не вышло забрать данные даже с сдвигом {shift_rv}') from None

            # проверка сформированного файла
            if file_exists(csv_file) and file_size(csv_file) > 20:  # 20 bytes empty gzipped data
                log.info(f'found file with size {file_size(csv_file)}')

                count_rows_csv = gp_bash.run(f'zgrep -c ^ {csv_file}', return_stdout=True)
                count_rows_csv = int(count_rows_csv)
            else:
                count_rows_csv = 0

            if count_rows_csv < 5000000:
                shift_rv = shift_rv * 2  # если мало данных в файле, увеличиваем отсечку для следующих циклов
                log.info(f'Новый сдвиг {shift_rv}')

            if count_rows_csv > 20000000:
                shift_rv = round(shift_rv / 2)  # если много данных в файле, уменьшаем отсечку для следующих циклов
                log.info(f'Новый сдвиг {shift_rv}')

            log.info(f'count rows in file {csv_file} = {count_rows_csv}')

            if (int(count_rows_csv) if count_rows_csv else 0) == 0:
                if file_exists(csv_file):
                    os.remove(f'{csv_file}')
                log.info('No Data, save po...')
                with try_conn(connect_gp) as conn:
                    cursor = conn.cursor()
                    log.info(f'update max set {po}')
                    if debug == 0:
                        cursor.execute(
                            f'''insert into max_val_cutoff(table_name, max_int) values('{cutoff_name}', {po})''')
                    else:
                        cursor.execute(
                            f'''insert into test_izotov.max_val_cutoff(table_name, max_int) values('{cutoff_name}', {po})''')
            else:
                log.info(f' new_max  = {po}')
                try:
                    if buffer_table:
                        with try_conn(connect_gp) as conn:
                            cursor = conn.cursor()
                            log.info(f'truncate table {buffer_table};')
                            cursor.execute(f'''truncate table {buffer_table};''')
                    gp_bash.run(gp_command, displayed_bash_command="Load from csv")
                    log.info('Done!')
                    with try_conn(connect_gp) as conn:
                        cursor = conn.cursor()
                        if delete_script:  # скрипт выполняющийся для очистки данных перед вставкой
                            log.info(delete_script)
                            cursor.execute(delete_script)
                        if replace_insert_script:  # этот скрипт используется ВМЕСТО стандартного скрипта вставки/можно даже апдейтить
                            log.info(replace_insert_script)
                            cursor.execute(replace_insert_script)
                        else:
                            if buffer_table:
                                if on_conflict == 0:
                                    log.info(f'insert new rows in {recipient_table};')
                                    cursor.execute(f"""
                                                    insert into {recipient_table} ({columns})
                                                    select {columns}   from {buffer_table} ;
                                                    """)
                                else:
                                    log.info(f'insert ON CONFLICT DO NOTHING in {recipient_table};')
                                    cursor.execute(f"""INSERT INTO {recipient_table} ({columns})
                                                    SELECT {columns} FROM {buffer_table}
                                                    ON CONFLICT DO NOTHING;""")
                        log.info(f'update max set {po}')
                        if debug == 0:
                            cursor.execute(
                                f'''insert into max_val_cutoff(table_name, max_int) values('{cutoff_name}', {po})''')
                        else:
                            cursor.execute(
                                f'''insert into test_izotov.max_val_cutoff(table_name, max_int) values('{cutoff_name}', {po})''');
                        log.info(f'Done {recipient_table}')
                        os.remove(f'{csv_file}')
                except Exception as e:
                    log.info(e)
                    os.remove(f'{csv_file}')
                    raise AirflowException('process_data() failed with exception error %s' % e.__repr__()) from None
            current_cutoff = po
            if current_cutoff == end_cutoff:
                return f'all updated to {end_cutoff}'
            if current_cutoff + shift_rv <= end_cutoff:
                po = current_cutoff + shift_rv
            else:
                po = end_cutoff
    # :
    if created_cutoff >= end_cutoff:
        return 'nothing to update'
    else:
        return f'updated for {po}'


def copy_to_kh_csv(
        src_connection: str  # 'do-greenplum'
        , dst_connection: str  # 'do-ch-dm0'
        , src_table_name: str  # таблица на гп/пг
        , dst_table_name: str  # таблица на кх
        , columns: str  # одинаковые имена колонок в селекте и в таблице на кх
        , buff_table_name: str = ''  # буферная таблица, если делается через неё вставка.
        , partition_list: list = [] # список партиций для замены
        , need_trunc: str = 'yes'  # необходимость в транкейте таблицы-назначения
        , db_type: str = 'gp'  # Выбор субд источник (pg-postgres, gp-greenplum).
        , select_from_gp : str = '' # Замена выборки из гп, колонки обязательно должны совпадать с columns
):
    """
        Функция копирующая данные из гп (src_connection) на кликхауз (dst_connection)
        через буферную таблицу (buff_table_name) (вариабельно)
        , в таблицу назначения (dst_table_name)
        из таблицы или вьюхи источника (src_table_name) причем в имя таблицы источника можно добавить where
        например datamart_shk_tracker.late_rids_duration_before_poo_agg where measure=1
        но обязательно не должно быть в строке спецсимволов, символов переноса каретки. всё должно быть в одну строку.
        в буферной таблице
        все поля идут в том же порядке и их ровно столько, сколько подаётся на вход
        в переменной (columns)
    """

    if not buff_table_name and partition_list: # Попытка замены партиций без буфера
        raise ValueError("Менять партиции можно только через буфер")

    # коннект до clickhouse через клиент
    ch_conn = '| clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
              ' --password="{password}" --format_csv_delimiter=";" --query="{insert_ch}" --receive_timeout=3600'
    # коннект до ГП через клиент
    gp_conn_con = 'PGPASSWORD="{password}" psql -h {host} -p {port} -U {login} -d {schema} -v ON_ERROR_STOP=1' \
                  ' -c "{copy_gp}"'
    # скрипт забора данных с GP/PG
    if select_from_gp:
        select_script = select_from_gp
    else:
        select_script = f'select {columns} from {src_table_name} '
    # команды для копирования и вставки данных с GP/PG на витринный clickhouse
    if db_type == 'gp':
        copy_gp = f"\copy ({select_script}) to stdout with (HEADER TRUE, ENCODING 'UTF8')"
        insert_ch = f'INSERT INTO {buff_table_name if buff_table_name else dst_table_name} FORMAT TSVWithNames'
    else:
        copy_gp = f"\copy ({select_script}) to stdout with (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8')"
        insert_ch = f'INSERT INTO {buff_table_name if buff_table_name else dst_table_name} FORMAT CSVWithNames'
    ch_bash = BashHook(dst_connection)
    gp_bash = BashHook(src_connection)

    raw_bash = BashHook(None)

    gp_command = gp_bash.render_command(gp_conn_con, copy_gp=copy_gp)
    ch_command = ch_bash.render_command(ch_conn, insert_ch=insert_ch)

    gp_command_display = gp_bash.render_command(gp_conn_con, hide_password=True, copy_gp=copy_gp)
    ch_command_display = ch_bash.render_command(ch_conn, hide_password=True, insert_ch=insert_ch)

    displayed_bash_command = gp_command_display + ch_command_display
    bash_command = gp_command + ch_command

    if buff_table_name and not partition_list: # Загрузка без партиций:
        ch_hook = ClickhouseHook(dst_connection)
        with ch_hook.get_conn(send_receive_timeout=3600).cursor() as ch_cur:
            ch_cur.execute("SET mutations_sync = 1;")  # Должен заставлять клиента дожидаться выполнения команды
            ch_cur.execute(f"alter table {buff_table_name} delete where 1=1;")
            log.info(f'ожидаем 5 сек делит буферки ')
            time.sleep(5)
            log.info(f'truncate {buff_table_name} ')
            # удаление данных из буферной
            ch_cur.execute(f'truncate table {buff_table_name};')
        log.info(f'Start loading to {buff_table_name}...')
        raw_bash.run(bash_command=bash_command, displayed_bash_command=displayed_bash_command)
        with ch_hook.get_conn(send_receive_timeout=3600).cursor() as ch_cur:
            if need_trunc == 'yes':
                log.info(f'truncate table {dst_table_name}...')
                ch_cur.execute("SET mutations_sync = 1;")
                ch_cur.execute(f"alter table {dst_table_name} delete where 1=1;")
                log.info(f'ожидаем 5 сек делит таблицы назначения ')
                time.sleep(5)
                ch_cur.execute(f'truncate table {dst_table_name};')
            # обновление таблицы назначения
            log.info(f'start loading to {dst_table_name}...')
            columns = columns.replace('::int', '')  # убираем преобразование типов из колумнов
            ch_cur.execute(f"""insert into {dst_table_name} ({columns})
                        select {columns} from {buff_table_name};""")
        log.info(f'Data moved successfully to {dst_table_name}')
    if buff_table_name and partition_list: # Загрузка заменой партиций
        ch_hook = ClickhouseHook(dst_connection)
        with ch_hook.get_conn(send_receive_timeout=3600).cursor() as ch_cur:
            ch_cur.execute("SET mutations_sync = 1;")  # Должен заставлять клиента дожидаться выполнения команды
            ch_cur.execute(f"alter table {buff_table_name} delete where 1=1;")
            log.info(f'ожидаем 5 сек делит буферки ')
            time.sleep(5)
            log.info(f'truncate {buff_table_name} ')
            # удаление данных из буферной
            ch_cur.execute(f'truncate table {buff_table_name};')
        log.info(f'Start loading to {buff_table_name}...')
        raw_bash.run(bash_command=bash_command, displayed_bash_command=displayed_bash_command)
        log.info(f'Start replacing partitions {partition_list} in {dst_table_name}...')
        for part in partition_list:
            with ch_hook.get_conn(send_receive_timeout=3600).cursor() as ch_cur:
                ch_cur.execute(f"ALTER TABLE {dst_table_name} REPLACE PARTITION {part} FROM {buff_table_name};")
            log.info(f'Replaced {part} in {dst_table_name}')
        log.info(f'Data moved successfully to {dst_table_name}')
    else:
        if need_trunc == 'yes':
            ch_hook = ClickhouseHook(dst_connection)
            with ch_hook.get_conn(send_receive_timeout=3600).cursor() as ch_cur:
                log.info(f'truncate table {dst_table_name}...')
                ch_cur.execute("SET mutations_sync = 1;")
                # обновление таблицы назначения
                ch_cur.execute(f'truncate table {dst_table_name};')
        log.info(f'Start loading to {dst_table_name}...')
        raw_bash.run(bash_command=bash_command, displayed_bash_command=displayed_bash_command)
        log.info(f'Data moved successfully to {dst_table_name}')

    log.info(f'All steps in {dst_table_name} are done')


def copy_ch_to_ch(recipient_table: str,  # таблица назначения, например stage_bo.transaction_transaction
                  take_data: str,  # скрипт забора из кликхауза, обязательно включающий  >={rv},<{po},FORMAT CSV
                  take_max: str,  # скрипт взятия максимальной даты/int64 из клика, возвращает int32/int64
                  columns: str,  # список полей в которые производится вставка shk_id,place_cod,box_id
                  shift_rv: int = 20000,  # сдвиг отсечки в секундах все отсечки - это датавремя в секундах.
                  src_ch: str = 'do-ch4',
                  dst_ch: str = 'do-ch2',
                  buffer_table: str = '',  # буферная таблица, для delete/insert
                  delete_script: str = '',  # скрипт очистки таблицы назначения
                  replace_insert_script: str = '',
                  # скрипт выполняемый прямо перед переброской из буфера в назначение
                  take_all: str = '',
                  cnt_while: int = 10,  # количество итераций в одном даге, чтобы не забирать прям всё и сразу
                  # если забираем всё (может буферка на клике), то all, если из обновляющейся, то смещаем на 10
                  debug: int = 0,  # Если дебаг, используем определённый test_izotov схему
                  cutoff_name: str = None # имя отсечки, если имя <recipient_table> занято или не подходит
                  ):
    """Копирование из клика в клик"""
    with TemporaryDirectory(prefix='dataex') as tmp_dir:
        csv_file = f'{tmp_dir}/{recipient_table}.csv.gz'
        # первая часть, формирование csv файла
        raw_bash = BashHook(None)
        # коннект до clickhouse через клиент
        ch_src_hook = ClickhouseHook(src_ch)
        ch_src_bash = BashHook(src_ch)
        ch_src_connect = ' clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
                         ' --password="{password}" --query="{take_data}" | gzip > {csv_file}'
        # коннект до clickhouse2 через клиент
        ch_dst_hook = ClickhouseHook(dst_ch)
        ch_dst_bash = BashHook(dst_ch)
        ch_dst_conn = f'zcat {csv_file}' + ' | clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
                                           ' --password="{password}" --format_csv_delimiter="," --query="{insert_ch}"'
        with ch_dst_hook.get_conn() as conn:
            cutoff_name = cutoff_name or recipient_table
            cursor = conn.cursor()
            log.info(f'Took last max from {dst_ch}')
            cursor.execute(f'''select max_int from public.last_max_val_cutoff where table_name='{cutoff_name}' ''')
            try:
                created_cutoff = cursor.fetchone()[0]
            except TypeError:
                created_cutoff = None
            log.info(f'created_cutoff={created_cutoff}')
        with ch_src_hook.get_conn() as conn:
            cursor = conn.cursor()
            if created_cutoff is None:
                cursor.execute(take_max.replace('max(', 'min('))
                created_cutoff = cursor.fetchone()[0]
                log.info(f'from take_max created_cutoff={created_cutoff}')
            log.info('Took max KH')
            cursor.execute(take_max)
            if take_all:
                end_cutoff = cursor.fetchone()[0] + 1
                # если есть значение в take_all, значит высасываем всё что там есть.
            else:
                end_cutoff = cursor.fetchone()[0] - 10  # смещаем на 10 секунд, чтобы не терять ещё вставляющиеся.
        if end_cutoff < created_cutoff:
            return 0
        current_cutoff = created_cutoff
        if current_cutoff + shift_rv <= end_cutoff:
            po = current_cutoff + shift_rv
        else:
            po = end_cutoff
        all_count_rows = 0
        if take_all:
            cnt_while_current = -10000
        else:
            cnt_while_current = 0
        while po <= end_cutoff:
            try:
                log.info(f'Отсечка с {datetime.utcfromtimestamp(current_cutoff)} по {datetime.utcfromtimestamp(po)}')
            except:
                log.info(f'Отсечка с {current_cutoff} по {po}')
            #  часта ошибка с переполнением буфера клика, потому пробуем уменьшать отсечку для уменьшения потребления памяти.
            try_push = 0
            while shift_rv > 10 and try_push == 0:
                try:
                    ch_command = ch_src_bash.render_command(ch_src_connect
                                                            , take_data=take_data.format(rv=current_cutoff, po=po)
                                                            , csv_file=csv_file)
                    ch_src_bash.run(ch_command, displayed_bash_command='Export to csv')
                    try_push = 1
                except Exception as e:
                    log.info(e)
                    shift_rv = round(shift_rv / 2)
                    log.info(f'Пробуем сдвиг {shift_rv}')
                    po = current_cutoff + shift_rv

            if shift_rv <= 10:
                raise AirflowException(f'Не вышло забрать данные даже с сдивгом {shift_rv}') from None

            # проверка сформированного файла
            if file_exists(csv_file) and file_size(csv_file) > 20:  # 20 bytes empty gzipped data
                log.info(f'found file with size {file_size(csv_file)}')
                count_rows_csv = ch_src_bash.run(f'zgrep -c ^ {csv_file}', return_stdout=True)
                count_rows_csv = int(count_rows_csv)
            else:
                count_rows_csv = 0

            if count_rows_csv < 5000000:
                shift_rv = shift_rv * 2  # если мало данных в файле, увеличиваем отсечку для следующих циклов
                log.info(f'Новый сдвиг {shift_rv}')

            if count_rows_csv > 15000000:
                shift_rv = round(shift_rv / 2)  # если достаточно строк, понизим, чтобы оптимизировать количество.
                log.info(f'Новый сдвиг {shift_rv}')

            log.info(f'count rows in file {csv_file} = {count_rows_csv}')

            if (int(count_rows_csv) if count_rows_csv else 0) < 2:  # 1 запись - это заголовок
                os.remove(f'{csv_file}')
                log.info('No Data, save po...')
                with ch_dst_hook.get_conn() as conn:
                    cursor = conn.cursor()
                    log.info(f'update max set {po}')
                    cursor.execute(f"""insert into public.max_val_cutoff(table_name, max_int) values('{cutoff_name}',{po})""")
            else:
                all_count_rows = all_count_rows + count_rows_csv - 1
                log.info(f' new_max  = {po}')
                try:
                    if buffer_table:
                        with ch_dst_hook.get_conn(send_receive_timeout=3600).cursor() as ch_cur:
                            ch_cur.execute(
                                "SET mutations_sync = 1;")  # Должен заставлять клиента дожидаться выполнения команды
                            log.info(f'truncate table {buffer_table};')
                            ch_cur.execute(f'''truncate table {buffer_table};''')
                    insert_ch = f'INSERT INTO {buffer_table if buffer_table else recipient_table} ({columns}) FORMAT CSVWithNames'
                    ch_command = ch_dst_bash.render_command(ch_dst_conn
                                                            , insert_ch=insert_ch)
                    ch_command_display_debug = ch_dst_bash.render_command(ch_dst_conn
                                                                          , hide_password=True
                                                                          , insert_ch=insert_ch)
                    raw_bash.run(bash_command=ch_command, displayed_bash_command="Load from csv")
                    with ch_dst_hook.get_conn() as conn:
                        cursor = conn.cursor()
                        if delete_script:  # скрипт выполняющийся для очистки данных перед вставкой
                            log.info(delete_script)
                            cursor.execute(delete_script)
                        if replace_insert_script:  # этот скрипт используется ВМЕСТО стандартного скрипта вставки/можно даже апдейтить
                            log.info(replace_insert_script)
                            cursor.execute(replace_insert_script)
                        else:
                            if buffer_table:
                                log.info(f'insert new rows in {recipient_table};')
                                cursor.execute(f"""
                                                insert into {recipient_table} ({columns})
                                                select {columns}   from {buffer_table} ;
                                                """)
                        log.info(f'update max set {po}')
                        cursor.execute(
                            f'''insert into public.max_val_cutoff(table_name, max_int) values('{cutoff_name}', {po})''')
                        log.info(f'Done {recipient_table}')
                        os.remove(f'{csv_file}')
                except Exception as e:
                    log.info(e)
                    log.info(ch_command_display_debug)
                    os.remove(f'{csv_file}')
                    raise AirflowException('process_data() failed with exception error %s' % e.__repr__()) from None
            current_cutoff = po
            cnt_while_current += 1
            if cnt_while_current > cnt_while:  # закончили итерации
                return all_count_rows
            if current_cutoff == end_cutoff:
                return all_count_rows
            if current_cutoff + shift_rv <= end_cutoff:
                po = current_cutoff + shift_rv
            else:
                po = end_cutoff


def copy_ch_to_ch_pipe(
                  take_data: str,  # скрипт забора из кликхауза, обязательно включающий FORMAT MsgPack и не включающий ";"
                  insert_data: str,  # скрипт вставки в кликхауз, обязательно включающий FORMAT MsgPack и не включающий ";"
                  src_ch: str = 'do-ch4',
                  dst_ch: str = 'do-ch2',
                  debug: bool = False, # если True - то только рендерим команду, но не исполняем
                  throw_if_empty: bool = False, # если True - выкинет ошибку если запрос take_data вернёт ноль строк
                  multiquery: bool = False # если True, в take_data может быть несколько запросов через ";", последний - селект
                  ):
    """Копирование из клика в клик напрямую через |"""
    raw_bash = BashHook(None)
    ch_src_bash = BashHook(src_ch)
    ch_src_conn = 'clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
                     ' --password="{password}" --query="{take_data}"'
    ch_dst_bash = BashHook(dst_ch)
    ch_dst_conn = 'clickhouse-client --host="{host}" --port="{port}" --database="{schema}" --user="{login}"' \
                                       ' --password="{password}" --format_csv_delimiter="," --query="{insert_data}"' \
                                       ' --throw_if_no_data_to_insert="{throw_if_empty}"' 
    if multiquery:
        ch_src_conn += ' --multiquery'
        ch_dst_conn += ' --multiquery'

    ch_src_command = ch_src_bash.render_command(ch_src_conn, take_data=take_data)
    ch_src_command_display = ch_src_bash.render_command(ch_src_conn, hide_password=True, take_data=take_data)

    ch_dst_command = ch_dst_bash.render_command(ch_dst_conn, insert_data=insert_data, throw_if_empty=int(throw_if_empty))
    ch_dst_command_display = ch_dst_bash.render_command(ch_dst_conn, hide_password=True, insert_data=insert_data, throw_if_empty=int(throw_if_empty))

    ch_command = ch_src_command + " | " + ch_dst_command
    ch_command_display = ch_src_command_display + " | " + ch_dst_command_display

    if not debug:
        raw_bash.run(ch_command, displayed_bash_command=ch_command_display)
    else:
        log.info(f'Debug enabled! Rendered command: ')
        log.info(ch_command_display)
    log.info(f'Done !')
