--Раскрыть массив из поля Array[Int32]
SELECT
    status_oof,
    arrayJoin(statuses_v1) AS status_v1_element from dict_final.positions_statuses_oof;
