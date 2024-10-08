--преобразование текста в число:
toInt64(halfMD5(srid)) as rid

-- множественный case
                                       multiIf(wh_tare_entry = 'ERP', 1,
                                                wh_tare_entry = 'LOGISTICS', 2,
                                                wh_tare_entry = 'WBGO', 3,
                                                wh_tare_entry = 'WH_RPK', 4,
                                                wh_tare_entry = 'PVZ', 5,
                                                wh_tare_entry = 'PPVZ',6,
                                                wh_tare_entry = 'WH_TRBX', 7,
                                                wh_tare_entry = 'WBBOX', 8, 99) as wh_tare_entry_id,

--округление до часа, чтобы все записи этого часа учитывались
toStartOfHour(date_on_shelf+interval '1' HOUR) 

--проверка на перенос строки
if(position('\n' IN tare_sticker) > 0, 1, 0)

--способы вычисления наибольшего времени:
select arrayMax([now(),now()-interval '1' DAY]);
greatest
взять даты в такие конструкции reinterpretAsUInt64()
, тогда будут выводиться текущие время и дата или численное представление даты, конвертируемое обратно с помощью reinterpretAsDateTime()

SELECT toDateTime(max2(toUnixTimestamp(now()), toUnixTimestamp(now() - INTERVAL '1' DAY)))

with cte as (select now() as m1,toDateTime('2024-10-07') as m2, null as m3)
select greatest(m1,m2,m3),arrayMax([m1,m2]) ,arrayMin([m1,m2])
,arrayReduce('max', [m1,m2,m3]) -- лучшие варианты, отбрасывает нуллы а из остальных считает
,arrayReduce('min', [m1,m2,m3])
from cte
;
