DO $$
DECLARE
    tbl text;           -- Имя таблицы
    yr int;             -- Год партиции
    partition_name text;-- Полное имя партиции
    start_date text;    -- Начало диапазона партиции
    end_date text;      -- Конец диапазона партиции
    index_exists int;   -- Флаг существования индекса
BEGIN
    -- Цикл по всем таблицам
    FOR tbl IN SELECT unnest(ARRAY['bm_st_init', 'bm_en_final', 'bm_ex_complect', 'bm_ex_decomplect', 
                                   'bm_ex_inventory', 'bm_ex_movement', 'bm_ex_resort', 'bm_ex_sale', 
                                   'bm_ex_update', 'bm_ex_write_off', 'bm_in_complect', 'bm_in_decomplect', 
                                   'bm_in_entering', 'bm_in_inventory', 'bm_in_movement', 'bm_in_receipt', 
                                   'bm_in_resort', 'bm_in_update', 'bm_tr_transfer'])
    LOOP
        -- Цикл по годам (2020–2025)
        FOR yr IN 2020..EXTRACT(YEAR FROM CURRENT_DATE)::int
        LOOP
            partition_name := format('%s_%s', tbl, yr);
            start_date := format('%s-01-01', yr);
            end_date := format('%s-01-01', yr + 1);

            -- Проверка и создание партиции
            IF NOT EXISTS (
                SELECT 1 
                FROM pg_class 
                WHERE relname = partition_name 
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'partitions')
            ) THEN
                EXECUTE format(
                    'CREATE TABLE partitions.%s PARTITION OF report.%s FOR VALUES FROM (''%s'') TO (''%s'')',
                    partition_name, tbl, start_date, end_date
                );
                RAISE NOTICE 'Создана партиция: partitions.%', partition_name;
            END IF;

            -- Проверка существования индекса по date
            SELECT COUNT(*) INTO index_exists
            FROM pg_indexes
            WHERE schemaname = 'partitions'
            AND tablename = partition_name
            AND indexdef LIKE '%(date)';
            IF index_exists = 0 THEN
                EXECUTE format(
                    'CREATE INDEX CONCURRENTLY idx_%s_%s_date ON partitions.%s_%s (date)',
                    tbl, yr, tbl, yr
                );
                RAISE NOTICE 'Создан индекс idx_%s_%s_date', tbl, yr;
            END IF;

            -- Проверка индекса по sku_id_1c, date
            SELECT COUNT(*) INTO index_exists
            FROM pg_indexes
            WHERE schemaname = 'partitions'
            AND tablename = partition_name
            AND indexdef LIKE '%(sku_id_1c, date)';
            IF index_exists = 0 THEN
                EXECUTE format(
                    'CREATE INDEX CONCURRENTLY idx_%s_%s_sku_date ON partitions.%s_%s (sku_id_1c, date)',
                    tbl, yr, tbl, yr
                );
                RAISE NOTICE 'Создан индекс idx_%s_%s_sku_date', tbl, yr;
            END IF;

            -- Проверка индекса по wh_id_1c, date
            SELECT COUNT(*) INTO index_exists
            FROM pg_indexes
            WHERE schemaname = 'partitions'
            AND tablename = partition_name
            AND indexdef LIKE '%(wh_id_1c, date)';
            IF index_exists = 0 THEN
                EXECUTE format(
                    'CREATE INDEX CONCURRENTLY idx_%s_%s_wh_date ON partitions.%s_%s (wh_id_1c, date)',
                    tbl, yr, tbl, yr
                );
                RAISE NOTICE 'Создан индекс idx_%s_%s_wh_date', tbl, yr;
            END IF;

            -- Проверка индекса по ca_id_1c, date
            SELECT COUNT(*) INTO index_exists
            FROM pg_indexes
            WHERE schemaname = 'partitions'
            AND tablename = partition_name
            AND indexdef LIKE '%(ca_id_1c, date)';
            IF index_exists = 0 THEN
                EXECUTE format(
                    'CREATE INDEX CONCURRENTLY idx_%s_%s_ca_date ON partitions.%s_%s (ca_id_1c, date)',
                    tbl, yr, tbl, yr
                );
                RAISE NOTICE 'Создан индекс idx_%s_%s_ca_date', tbl, yr;
            END IF;
        END LOOP;

        -- Обновление статистики
        EXECUTE format('ANALYZE report.%s', tbl);
        RAISE NOTICE 'Обновлена статистика для report.%', tbl;
    END LOOP;

    RAISE NOTICE 'Все партиции и индексы обработаны, дубликаты исключены.';
END $$;