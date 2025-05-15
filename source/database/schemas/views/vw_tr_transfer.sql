-- report.vw_tr_transfer исходный текст

CREATE OR REPLACE VIEW report.vw_tr_transfer
AS WITH pre_agg AS (
         SELECT bm_tr_transfer.date,
            bm_tr_transfer.day,
            bm_tr_transfer.week,
            bm_tr_transfer.month,
            bm_tr_transfer.quarter AS quarted,
            bm_tr_transfer.year,
            bm_tr_transfer.wh_id_1c,
            bm_tr_transfer.wh_name_1c,
            bm_tr_transfer.ca_id_1c,
            bm_tr_transfer.ca_name_1c,
            bm_tr_transfer.sku_id_1c,
            sum(bm_tr_transfer.cnt) AS cnt,
            sum(bm_tr_transfer.sc_rub) AS sc_rub,
            sum(bm_tr_transfer.sc_euro) AS sc_euro,
            sum(bm_tr_transfer.sku_sc_rub) AS sku_sc_rub,
            sum(bm_tr_transfer.sku_sc_euro) AS sku_sc_euro
           FROM report.bm_tr_transfer
          GROUP BY bm_tr_transfer.date, bm_tr_transfer.day, bm_tr_transfer.week, bm_tr_transfer.month, bm_tr_transfer.quarter, bm_tr_transfer.year, bm_tr_transfer.wh_id_1c, bm_tr_transfer.wh_name_1c, bm_tr_transfer.ca_id_1c, bm_tr_transfer.ca_name_1c, bm_tr_transfer.sku_id_1c
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