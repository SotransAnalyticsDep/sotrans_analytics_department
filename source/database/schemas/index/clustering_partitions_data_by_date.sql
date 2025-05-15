DO $$
DECLARE
    tbl text;           -- Имя таблицы
    yr int;             -- Год партиции
    partition_name text;-- Полное имя партиции
    index_name text;    -- Имя индекса
    start_time timestamp; -- Время начала
BEGIN
    -- Цикл по всем таблицам
    FOR tbl IN SELECT unnest(ARRAY['bm_st_init', 'bm_en_final', 'bm_ex_complect', 'bm_ex_decomplect',
                                   'bm_ex_inventory', 'bm_ex_movement', 'bm_ex_resort', 'bm_ex_sale',
                                   'bm_ex_update', 'bm_ex_write_off', 'bm_in_complect', 'bm_in_decomplect',
                                   'bm_in_entering', 'bm_in_inventory', 'bm_in_movement', 'bm_in_receipt',
                                   'bm_in_resort', 'bm_in_update', 'bm_tr_transfer'])
    LOOP
        -- Цикл по годам (2020–2025)
        FOR yr IN 2020..2025
        LOOP
            partition_name := format('%s_%s', tbl, yr);
            index_name := format('idx_%s_%s_date', tbl, yr);

            IF EXISTS (
                SELECT 1
                FROM pg_class
                WHERE relname = partition_name
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'partitions')
            ) THEN
                IF EXISTS (
                    SELECT 1
                    FROM pg_indexes
                    WHERE schemaname = 'partitions'
                    AND tablename = partition_name
                    AND indexname = index_name
                ) THEN
                    -- Записываем время начала
                    start_time := clock_timestamp();
                    -- Кластеризация
                    EXECUTE format('CLUSTER partitions.%s USING %s', partition_name, index_name);
                    RAISE NOTICE 'Кластеризована партиция: partitions.%s по индексу %s, время: %', 
                                 partition_name, index_name, clock_timestamp() - start_time;
                ELSE
                    RAISE NOTICE 'Индекс %s для partitions.%s не найден, пропускаем', index_name, partition_name;
                END IF;

                -- Обновление статистики
                EXECUTE format('ANALYZE partitions.%s', partition_name);
                RAISE NOTICE 'Обновлена статистика для partitions.%s', partition_name;
            ELSE
                RAISE NOTICE 'Партиция partitions.%s не существует, пропускаем', partition_name;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'Кластеризация всех партиций завершена.';
END $$;