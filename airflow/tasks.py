# параметры таски

    avoid_lock_for_deleted_task = ExternalLastTaskSensor( # обёртка над  ExternalTaskSensor /прослушивает таски внешние
        pool='do-services', # пул, ограничивает одновременную работу тасков
        task_id='avoid_lock_for_deleted', # айди этой таски
        external_dag_id='gp_load_inc_ch2_datamart_shk_last_state', # айди дага который слушать надо
        external_task_ids=['task_load_to_gp_place_deleted', 'load_to_gp_place_deleted_from_positions'], # какие таски из того дага нас интересуют
        soft_fail=True, # если те даги выпали в ошибку. то эту таску скипнуть
        poke_interval=float(60),    # стучаться каждые 60 секунд, воркера не отпускать
        timeout=float(60*20),       # 20 минут ожидания максимум
    )


    upd_shk_on_place_deleted_task = PythonOperator(
        task_id="upd_shk_on_place_deleted",
        dag=dag,
        doc=f"Обновляем upd_shk_on_place_deleted, основываясь на последних данных inventory_lost_shk_all",
        python_callable=upd_shk_on_place_deleted,
        pool=GP_CONN_ID,
        trigger_rule='all_success', # выполняется только если все предыдущие не скипнулись и не упали. (дефолт)
        inlets={"datasets": [
            Dataset("greenplum", "drr-gp-master.dwh.stage_external.inventory_lost_shk_all"),
        ],
