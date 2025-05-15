DO $$
DECLARE
    tbl text;
    yr int;
    partition_name text;
BEGIN
    FOR tbl IN SELECT unnest(ARRAY['bm_st_init', 'bm_en_final', 'bm_ex_complect', 'bm_ex_decomplect',
                                   'bm_ex_inventory', 'bm_ex_movement', 'bm_ex_resort', 'bm_ex_sale',
                                   'bm_ex_update', 'bm_ex_write_off', 'bm_in_complect', 'bm_in_decomplect',
                                   'bm_in_entering', 'bm_in_inventory', 'bm_in_movement', 'bm_in_receipt',
                                   'bm_in_resort', 'bm_in_update', 'bm_tr_transfer'])
    LOOP
        FOR yr IN 2020..2025
        LOOP
            partition_name := format('partitions.%s_%s', tbl, yr);
            EXECUTE format('CREATE TABLE IF NOT EXISTS %s PARTITION OF report.%s FOR VALUES FROM (''%s-01-01'') TO (''%s-01-01'')', 
                           partition_name, tbl, yr, yr + 1);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_%s_date ON %s (date)', tbl, yr, partition_name);
        END LOOP;
    END LOOP;
END $$;