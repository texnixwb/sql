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
