-- report.vw_ex_resort исходный текст

CREATE OR REPLACE VIEW report.vw_ex_resort
AS WITH pre_agg AS (
         SELECT bm_ex_resort.date,
            bm_ex_resort.day,
            bm_ex_resort.week,
            bm_ex_resort.month,
            bm_ex_resort.quarter AS quarted,
            bm_ex_resort.year,
            bm_ex_resort.wh_id_1c,
            bm_ex_resort.wh_name_1c,
            bm_ex_resort.ca_id_1c,
            bm_ex_resort.ca_name_1c,
            bm_ex_resort.sku_id_1c,
            sum(bm_ex_resort.cnt) AS cnt,
            sum(bm_ex_resort.sc_rub) AS sc_rub,
            sum(bm_ex_resort.sc_euro) AS sc_euro,
            sum(bm_ex_resort.sku_sc_rub) AS sku_sc_rub,
            sum(bm_ex_resort.sku_sc_euro) AS sku_sc_euro
           FROM report.bm_ex_resort
          GROUP BY bm_ex_resort.date, bm_ex_resort.day, bm_ex_resort.week, bm_ex_resort.month, bm_ex_resort.quarter, bm_ex_resort.year, bm_ex_resort.wh_id_1c, bm_ex_resort.wh_name_1c, bm_ex_resort.ca_id_1c, bm_ex_resort.ca_name_1c, bm_ex_resort.sku_id_1c
        )
 SELECT pa.date,
    pa.day,
    pa.week,
    pa.month,
    pa.quarted,
    pa.year,
    pa.wh_id_1c,
    pa.wh_name_1c,
    cwh.wh_name,
    pa.ca_id_1c,
    pa.ca_name_1c,
    cca.ca_name,
    cca.ca_type,
    cca.ca_status,
    cnm.parent_folder,
    pa.sku_id_1c,
    cnm.brand_id_1c,
    cnm.brand_name_1c,
    cnm.sku_name_1c,
    cnm.sku_cat_num,
    cnm.sku_art_num,
    pa.cnt,
    pa.sc_rub,
    pa.sc_euro,
    pa.sku_sc_rub,
    pa.sku_sc_euro
   FROM pre_agg pa
     LEFT JOIN constant.warehouse cwh ON pa.wh_id_1c = cwh.wh_id_1c::text
     LEFT JOIN constant.contragent cca ON pa.ca_id_1c = cca.ca_id_1c::text
     LEFT JOIN constant.nomenclature cnm ON pa.sku_id_1c = cnm.sku_id_1c;