--расчёт расстояния по прямой
round(sqrt(pow(
                                         69.1 *
                                         (lm.latitude - COALESCE(pvz.latitude, lm.latitude)),
                                         2::numeric)::double precision +
                             pow(
                                         (69.1 * (COALESCE(pvz.longitude, lm.longitude) - lm.longitude))::double precision *
                                         cos((lm.latitude / COALESCE(pvz.latitude, lm.latitude))::double precision),
                                         2::double precision)) *
                        1.6::double precision)

--преобразовать нестандартные типы дат в дату
--если dt='20.02.2023'
to_date(dt, 'DD.MM.YYYY')

--разделить строку на строки через разделитель
unnest(string_to_array(status_in_source, ','))
