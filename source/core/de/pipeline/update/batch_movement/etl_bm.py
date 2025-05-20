import os
import sys
import datetime as dt

import pandas as pd
import sqlalchemy as sa
from tqdm import tqdm

from loguru import logger

sys.path.append(r'C:\Users\user\Desktop\github\sotrans_analytics_department')
from source.core.de.connectors import PGConnector
from source.utils.decorators.handlers.excel import handler_xl_shared_string_xml
from source.utils.validation.database import CheckExistsDataInTable

logger.remove()
logger.add(sink=sys.stderr, level="ERROR")

pd.set_option('future.no_silent_downcasting', True)

engine = PGConnector().engine

NO_DATA: str = '_нет данных'

HEADERS: list[str] = [
    # Склады (wh -> warehouse)
    'wh_id_1c',
    'wh_name_1c',
    
    # Документ движения (dm -> document movement)
    'dm_date',
    'dm_id_1c',
    'dm_org',
    'dm_dep',
    'dm_type',
    
    # Документ партии (db -> document batch)
    'db_date',
    'db_id_1c',
    'db_org',
    'db_dep',
    'db_type',
    
    # Контрагент (ca -> contragent)
    'ca_id_1c',
    'ca_name_1c',
    
    # Номенклатура (sku -> ???)
    'sku_id_1c',
    
    # Начальный остаток (stb -> start balance)
    'stb_sku_sc_rub',
    'stb_cnt',
    'stb_sum_sc_rub',
    'stb_sum_sc_euro',
    
    # Приход (inb -> income balance)
    'inb_sku_sc_rub',
    'inb_cnt',
    'inb_sum_sc_rub',
    'inb_sum_sc_euro',
    
    # Расход (exb -> expend balance)
    'exb_sku_sc_rub',
    'exb_cnt',
    'exb_sum_sc_rub',
    'exb_sum_sc_euro',
    
    # Конечный остаток (enb -> end balance)
    'enb_sku_sc_rub',
    'enb_cnt',
    'enb_sum_sc_rub',
    'enb_sum_sc_euro',
]

SRC_PATH: str = r'C:\Users\user\YandexDisk\batch_movement\batch_movement'

# Функции
def filldown_in_dm(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Заполнение пропусков в столбцах "dm_date", "dm_id_1c", "dm_org", "dm_dep", и "dm_type" методом "вниз"')
    df.loc[:, 'dm_date'] = df['dm_date'].ffill()
    df.loc[:, 'dm_id_1c'] = df['dm_id_1c'].ffill()
    df.loc[:, 'dm_org'] = df['dm_org'].ffill()
    df.loc[:, 'dm_dep'] = df['dm_dep'].ffill()
    df.loc[:, 'dm_type'] = df['dm_type'].ffill()
    logger.success('Пропуски в столбцах "dm_date", "dm_id_1c", "dm_org", "dm_dep", и "dm_type" успешно заполнены методом "вниз"')
    
    return df

def filldown_in_db(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Заполнение пропусков в столбцах "db_date", "db_id_1c", "db_org", "db_dep", и "db_type" методом "вниз"')
    df.loc[:, 'db_date'] = df['db_date'].ffill()
    df.loc[:, 'db_id_1c'] = df['db_id_1c'].ffill()
    df.loc[:, 'db_org'] = df['db_org'].ffill()
    df.loc[:, 'db_dep'] = df['db_dep'].ffill()
    df.loc[:, 'db_type'] = df['db_type'].ffill()
    logger.success('Пропуски в столбцах "db_date", "db_id_1c", "db_org", "db_dep", и "db_type" успешно заполнены методом "вниз"')
    
    return df

def filldown_in_ca(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Заполнение пропусков в столбцах "ca_id_1c", "ca_name_1c" методом "вниз"')
    df.loc[:, 'ca_id_1c'] = df['ca_id_1c'].ffill()
    df.loc[:, 'ca_name_1c'] = df['ca_name_1c'].ffill()
    logger.success('Пропуски в столбцах "ca_id_1c", "ca_name_1c" успешно заполнены методом "вниз"')
    
    return df

def dropna_in_sku_id(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Удаление пропусков в столбце "sku_id_1c"')
    df = df.dropna(subset='sku_id_1c')
    logger.success('Пропуски в столбце "sku_id_1c" успешно удалены')
    
    return df

for filepath in tqdm(
    iterable=os.listdir(path=SRC_PATH),
    ncols=100,
    desc='Обработка XLSX-файлов'
):
    if not filepath.endswith('.xlsx') or filepath.startswith('~'):
        continue
    
    report_date = filepath.rsplit(sep='\\', maxsplit=1)[-1][:10]
    
    if CheckExistsDataInTable().batch_movement_by_date(prefix='st', doc_type='init', report_date=report_date):
        logger.info(f'Данные за {report_date} есть в базе')
        continue
    
    logger.info(f'\nНачало обработки данных {filepath}')
    @handler_xl_shared_string_xml
    def create_dataframe(filepath: str) -> pd.DataFrame:
        logger.debug('Старт формирования датафрейма')
        df: pd.DataFrame = pd.read_excel(
            io=filepath,
            engine='openpyxl',
            skiprows=6,
            skipfooter=1,
            usecols=range(6, 37),
            names=HEADERS,
            dtype='str'
        )
        logger.success('Датафрейм успешно сформирован')
        
        return df

    def add_report_date_column(df: pd.DataFrame, report_date: str | dt.datetime) -> pd.DataFrame:
        logger.debug('Старт добавления столбца с датой отчёта')
        df.loc[:, 'date'] = dt.datetime.strptime(report_date, '%Y.%m.%d')
        logger.success('Столбец с датой отчёта успешно добавлен')
        
        return df

    def add_report_day_column(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'day'] = [date.day for date in df['date']]
        
        return df

    def add_report_week_column(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'week'] = [date.week for date in df['date']]
        
        return df

    def add_report_month_column(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'month'] = [date.month for date in df['date']]
        
        return df

    def add_report_quarter_column(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'quarter'] = [date.quarter for date in df['date']]
        
        return df

    def add_report_year_column(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'year'] = [date.year for date in df['date']]
        
        return df

    def fill_miss_val_in_ca_name(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "ca_name"')
        df.loc[:, 'ca_name_1c'] = [
            NO_DATA
            if all([str(x) == 'nan' for x in (ca_name, sku_id)])
            else ca_name
            for ca_name, sku_id in zip(
                df.loc[:, 'ca_name_1c'],
                df.loc[:, 'sku_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "ca_name" успешно заполнены')
        
        return df

    def fill_miss_val_in_ca_id(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "ca_id_1c"')
        df.loc[:, 'ca_id_1c'] = [
            NO_DATA
            if ca_name == NO_DATA
            else ca_id
            for ca_name, ca_id in zip(
                df.loc[:, 'ca_name_1c'],
                df.loc[:, 'ca_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "ca_id_1c" успешно заполнены')
        
        return df

    def fill_miss_val_in_db_type(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "db_type"')
        df.loc[:, 'db_type'] = [
            NO_DATA
            if str(db_type).lower() == 'nan' and ca_id == NO_DATA
            else db_type
            for db_type, ca_id in zip(
                df.loc[:, 'db_type'],
                df.loc[:, 'ca_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "db_type" успешно заполнены')
        
        return df

    def fill_miss_val_in_db_dep(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "db_dep"')
        df.loc[:, 'db_dep'] = [
            NO_DATA
            if str(db_dep).lower() == 'nan' and ca_id == NO_DATA
            else db_dep
            for db_dep, ca_id in zip(
                df.loc[:, 'db_dep'],
                df.loc[:, 'ca_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "db_dep" успешно заполнены')
        
        return df

    def fill_miss_val_in_db_org(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "db_org"')
        df.loc[:, 'db_org'] = [
            NO_DATA
            if str(db_org).lower() == 'nan' and ca_id == NO_DATA
            else db_org
            for db_org, ca_id in zip(
                df.loc[:, 'db_org'],
                df.loc[:, 'ca_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "db_org" успешно заполнены')
        
        return df

    def fill_miss_val_in_db_id(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "db_id_1c"')
        df.loc[:, 'db_id_1c'] = [
            NO_DATA
            if str(db_id_1c).lower() == 'nan' and ca_id == NO_DATA
            else db_id_1c
            for db_id_1c, ca_id in zip(
                df.loc[:, 'db_id_1c'],
                df.loc[:, 'ca_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "db_id_1c" успешно заполнены')
        
        return df

    def fill_miss_val_in_db_date(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "db_date"')
        df.loc[:, 'db_date'] = [
            NO_DATA
            if str(db_date).lower() == 'nan' and ca_id == pd.NaT
            else db_date
            for db_date, ca_id in zip(
                df.loc[:, 'db_date'],
                df.loc[:, 'ca_id_1c']
            )
        ]
        logger.success('Пропуски в столбце "db_date" успешно заполнены')
        
        return df

    def fill_miss_val_in_dm_dep(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "dm_dep"')
        df.loc[:, 'dm_dep'] = [
            NO_DATA
            if str(dm_dep).lower() == 'nan' and db_date == NO_DATA
            else dm_dep
            for dm_dep, db_date in zip(
                df.loc[:, 'dm_dep'],
                df.loc[:, 'db_date']
            )
        ]
        logger.success('Пропуски в столбце "dm_dep" успешно заполнены')
        
        return df

    def fill_miss_val_in_dm_org(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "dm_org"')
        df.loc[:, 'dm_org'] = [
            NO_DATA
            if str(dm_org).lower() == 'nan' and db_date == NO_DATA
            else dm_org
            for dm_org, db_date in zip(
                df.loc[:, 'dm_org'],
                df.loc[:, 'db_date']
            )
        ]
        logger.success('Пропуски в столбце "dm_org" успешно заполнены')
        
        return df

    def fill_miss_val_in_dm_id(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "dm_id_1c"')
        df.loc[:, 'dm_id_1c'] = [
            NO_DATA
            if str(dm_id_1c).lower() == 'nan' and db_date == NO_DATA
            else dm_id_1c
            for dm_id_1c, db_date in zip(
                df.loc[:, 'dm_id_1c'],
                df.loc[:, 'db_date']
            )
        ]
        logger.success('Пропуски в столбце "dm_id_1c" успешно заполнены')
        
        return df

    def fill_miss_val_in_dm_date(df: pd.DataFrame,) -> pd.DataFrame:
        logger.debug('Заполнение пропусков в столбце "dm_date"')
        df.loc[:, 'dm_date'] = [
            date
            if str(dm_date).lower() == 'nan' and db_date == pd.NaT
            else date
            for dm_date, db_date, date in zip(
                df.loc[:, 'dm_date'],
                df.loc[:, 'db_date'],
                df.loc[:, 'date']
            )
        ]
        logger.success('Пропуски в столбце "dm_date" успешно заполнены')
        
        return df

    def filldown_in_wh_id(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков методом "вниз" в столбце "wh_id_1c"')
        df['wh_id_1c'] = df['wh_id_1c'].ffill()
        logger.success('Пропуски в столбце "wh_id_1c"успешно заполнены методом "вниз"')
        
        return df

    def filldown_in_wh_name(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug('Заполнение пропусков методом "вниз" в столбце "wh_name_1c"')
        df['wh_name_1c'] = df['wh_name_1c'].ffill()
        logger.success('Пропуски в столбце "wh_name_1c" успешно заполнены методом "вниз"')
        
        return df

    def change_digit_types(df: pd.DataFrame) -> pd.DataFrame:
        digit_cols: tuple[str, ...] = (
            'stb_sku_sc_rub', 'stb_cnt', 'stb_sum_sc_rub', 'stb_sum_sc_euro',
            'inb_sku_sc_rub', 'inb_cnt', 'inb_sum_sc_rub', 'inb_sum_sc_euro',
            'exb_sku_sc_rub', 'exb_cnt', 'exb_sum_sc_rub', 'exb_sum_sc_euro',
            'enb_sku_sc_rub', 'enb_cnt', 'enb_sum_sc_rub', 'enb_sum_sc_euro'
        )
        df = df.astype(dtype={x: 'float32' for x in digit_cols})
        
        return df

    def string_to_lower_case(df: pd.DataFrame) -> pd.DataFrame:
        for column_name in (
            'wh_id_1c',
            'wh_name_1c',
            'dm_id_1c',
            'dm_org',
            'dm_dep',
            'dm_type',
            'db_id_1c',
            'db_org',
            'db_dep',
            'db_type',
            'ca_id_1c',
            'ca_name_1c',
            'sku_id_1c'
        ):
            df[column_name] = df[column_name].str.lower()
        
        return df

    dataframe = (
        create_dataframe(filepath=os.path.join(SRC_PATH, filepath))
        .pipe(func=add_report_date_column, report_date=report_date)
        .pipe(func=add_report_day_column)
        .pipe(func=add_report_week_column)
        .pipe(func=add_report_month_column)
        .pipe(func=add_report_quarter_column)
        .pipe(func=add_report_year_column)
        .pipe(func=fill_miss_val_in_ca_name)
        .pipe(func=fill_miss_val_in_ca_id)
        .pipe(func=fill_miss_val_in_db_type)
        .pipe(func=fill_miss_val_in_db_dep)
        .pipe(func=fill_miss_val_in_db_org)
        .pipe(func=fill_miss_val_in_db_id)
        .pipe(func=fill_miss_val_in_db_date)
        .pipe(func=fill_miss_val_in_dm_dep)
        .pipe(func=fill_miss_val_in_dm_org)
        .pipe(func=fill_miss_val_in_dm_id)
        .pipe(func=fill_miss_val_in_dm_date)
        .pipe(func=filldown_in_wh_id)
        .pipe(func=filldown_in_wh_name)
        .pipe(func=change_digit_types)
        .pipe(func=string_to_lower_case)
    )

    # Список уникальных филиалов в файле
    new_warehouse = dataframe[['wh_id_1c', 'wh_name_1c']].drop_duplicates().dropna()

    # Загрузка списка уникальных магазинов из базы данных
    with engine.connect() as connection:
        pg_warehouse = pd.read_sql_table(
            table_name='warehouse',
            con=connection,
            schema='constant',
        )

    # Получения списка филиалов, которых нет в базе данных
    new_warehouse = new_warehouse[~new_warehouse['wh_id_1c'].isin(values=pg_warehouse['wh_id_1c'].unique())]

    # Загрузка новых филиалов в базу данных
    with engine.connect() as connection:
        new_warehouse.to_sql(
            name='warehouse',
            con=connection,
            schema='constant',
            index=False,
            chunksize=10_000,
            if_exists='append'
        )
        
    # Список уникальных контрагентов в файле
    new_contragent = dataframe[['ca_id_1c', 'ca_name_1c']].drop_duplicates().dropna()

    # Загрузка списка уникальных магазинов из базы данных
    with engine.connect() as connection:
        pg_contragent = pd.read_sql_table(
            table_name='contragent',
            con=connection,
            schema='constant',
        )

    # Получения списка филиалов, которых нет в базе данных
    new_contragent = new_contragent[~new_contragent['ca_id_1c'].isin(values=pg_contragent['ca_id_1c'].unique())]

    # Загрузка новых филиалов в базу данных
    with engine.connect() as connection:
        new_contragent.to_sql(
            name='contragent',
            con=connection,
            schema='constant',
            index=False,
            chunksize=10_000,
            if_exists='append'
        )
        
    # Загрузка справочника с наименованиями документов "хоз. операция"
    with engine.connect() as connection:
        pg_document_type: pd.DataFrame = pd.read_sql_table(
            table_name='document_type',
            con=connection,
            schema='constant',
        )

    # Уникальные документы в исходном DataFrame
    unique_src_doc_type = set(dataframe['dm_type'].dropna().unique())

    # Уникальные документы в базе данных
    unique_pg_doc_type = set(pg_document_type['doc_type'].unique())

    # Новые документы для добавления в базу
    new_doc_type = unique_src_doc_type - unique_pg_doc_type

    # Создание датафрейма с новыми документами
    df_new_doc_type = pd.DataFrame(data=new_doc_type, columns=['doc_type'])

    # Создание столбца с новыми наименованиями на англ. яз.
    df_new_doc_type['doc_type_name'] = None

    # Объединение новых значений с уже существующими
    pg_document_type = pd.concat([pg_document_type, df_new_doc_type])

    # Присвоение новых наименований
    pg_document_type.loc[:, 'doc_type_name'] = [
        input(f'Введите новое наименование на англ. яз. для документа: "{rus_doc_name}"')
        if eng_doc_name == None
        else eng_doc_name
        for rus_doc_name, eng_doc_name in zip(
            pg_document_type['doc_type'],
            pg_document_type['doc_type_name']
        )
    ]

    # Формирование словаря для замены наименований документов
    dict_to_replace = dict(zip(pg_document_type['doc_type'], pg_document_type['doc_type_name']))

    # Переименование документов
    dataframe['dm_type'] = dataframe['dm_type'].replace(dict_to_replace)

    # Сохранение новых документов в базу
    with engine.connect() as connection:
        (
            pg_document_type
            .to_sql(
                name='document_type',
                con=connection,
                schema='constant',
                if_exists='replace',
                index=False,
                chunksize=10_000
            )
        )
        
    df_source: pd.DataFrame = dataframe[~dataframe['wh_name_1c'].str.contains('товары в пути', case=False)]
    df_transfer: pd.DataFrame = dataframe[dataframe['wh_name_1c'].str.contains('товары в пути', case=False)]
    
    
    # ##################################################
    # НАЧАЛЬНЫЙ ОСТАТОК
    # ##################################################

    def fill_miss_val_stb_dm_type(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'dm_type'] = 'init'
        
        return df

    def drop_stb_cnt_nan(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset='stb_cnt')
        
        return df

    def drop_stb_cnt_below_zero(df: pd.DataFrame) -> pd.DataFrame:
        df = df.query('stb_cnt > 0')
        
        return df

    def fillna_in_stb_digit_cols(df: pd.DataFrame) -> pd.DataFrame:
        
        for column_name in ('stb_sku_sc_rub', 'stb_sum_sc_rub', 'stb_sum_sc_euro'):
            df.loc[:, column_name] = df[column_name].fillna(0.0)
        
        return df
    
    def rename_stb_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                'stb_sku_sc_rub': "sku_sc_rub",
                'stb_cnt': "cnt",
                'stb_sum_sc_rub': "sc_rub",
                'stb_sum_sc_euro': "sc_euro",
            }
        )
        
        return df
    
    def calc_sku_sc_euro(df: pd.DataFrame) -> pd.DataFrame:
        df['sku_sc_euro'] = [
            round(euro / cnt, 2)
            for euro, cnt in zip(
                df['sc_euro'],
                df['cnt']
            )
        ]
        
        return df

    df_start_balance: pd.DataFrame = (
        df_source
        [
            ['date', 'day', 'week', 'month', 'quarter', 'year']
            + HEADERS[:-16]
            + HEADERS[-16:-12]
        ]
    )

    df_start_balance = (
        df_start_balance
        .pipe(func=fill_miss_val_stb_dm_type)
        .pipe(func=filldown_in_dm)
        .pipe(func=filldown_in_db)
        .pipe(func=filldown_in_ca)
        .pipe(func=dropna_in_sku_id)
        .pipe(func=drop_stb_cnt_nan)
        .pipe(func=drop_stb_cnt_below_zero)
        .pipe(func=fillna_in_stb_digit_cols)
        .pipe(func=rename_stb_columns)
        .pipe(func=calc_sku_sc_euro)
    )

    for doc_type in df_start_balance['dm_type'].unique():
        with engine.connect() as connection:
            # if CheckExistsDataInTable().batch_movement_by_date(prefix='st', doc_type=doc_type, report_date=report_date):
            #     logger.info(f'Данные за {report_date} есть в базе')
                
            # else:
            df_start_balance.to_sql(
                name='bm_st_init',
                con=connection,
                schema='report',
                if_exists='append',
                index=False,
                chunksize=10_000
            )
    
    
    # ##################################################
    # ПОСТУПЛЕНИЕ ОСТАТКА
    # ##################################################
    def fill_miss_val_inb_dm_type(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'dm_type'] = 'income_balance'
        
        return df

    def drop_inb_cnt_nan(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset='inb_cnt')
        
        return df

    def drop_inb_cnt_below_zero(df: pd.DataFrame) -> pd.DataFrame:
        df = df.query('inb_cnt > 0')
        
        return df

    def fillna_in_inb_digit_cols(df: pd.DataFrame) -> pd.DataFrame:
        
        for column_name in ('inb_sku_sc_rub', 'inb_sum_sc_rub', 'inb_sum_sc_euro'):
            df.loc[:, column_name] = df[column_name].fillna(0.0)
        
        return df
    
    def rename_inb_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                'inb_sku_sc_rub': "sku_sc_rub",
                'inb_cnt': "cnt",
                'inb_sum_sc_rub': "sc_rub",
                'inb_sum_sc_euro': "sc_euro",
            }
        )
        
        return df
    
    def calc_sku_sc_euro(df: pd.DataFrame) -> pd.DataFrame:
        df['sku_sc_euro'] = [
            round(euro / cnt, 2)
            for euro, cnt in zip(
                df['sc_euro'],
                df['cnt']
            )
        ]
        
        return df

    df_income_balance: pd.DataFrame = (
        df_source
        [
            ['date', 'day', 'week', 'month', 'quarter', 'year']
            + HEADERS[:-16]
            + HEADERS[-12:-8]
        ]
    )

    df_income_balance = (
        df_income_balance
        .pipe(func=filldown_in_dm)
        .pipe(func=filldown_in_db)
        .pipe(func=filldown_in_ca)
        .pipe(func=dropna_in_sku_id)
        .pipe(func=drop_inb_cnt_nan)
        .pipe(func=drop_inb_cnt_below_zero)
        .pipe(func=fillna_in_inb_digit_cols)
        .pipe(func=rename_inb_columns)
        .pipe(func=calc_sku_sc_euro)
    )

    # Сохранение данных в базу
    for doc_type in df_income_balance['dm_type'].unique():
        with engine.connect() as connection:
            # if CheckExistsDataInTable().batch_movement_by_date(report_date):
            #     logger.info(f'Данные за {report_date} есть в базе')
            
            # else:
            (
                df_income_balance[df_income_balance['dm_type'] == doc_type]
                .to_sql(
                    name=f'bm_in_{doc_type}',
                    con=connection,
                    schema='report',
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )
            )
            logger.info(f'Документ: "{doc_type}" сохранён')
    
    
    # ##################################################
    # РАСХОД ОСТАТКА
    # ##################################################

    def fill_miss_val_exb_dm_type(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'dm_type'] = 'expend'
        
        return df

    def drop_exb_cnt_nan(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset='exb_cnt')
        
        return df

    def drop_exb_cnt_below_zero(df: pd.DataFrame) -> pd.DataFrame:
        df = df.query('exb_cnt > 0')
        
        return df

    def fillna_in_exb_digit_cols(df: pd.DataFrame) -> pd.DataFrame:
        
        for column_name in ('exb_sku_sc_rub', 'exb_sum_sc_rub', 'exb_sum_sc_euro'):
            df.loc[:, column_name] = df[column_name].fillna(0.0)
        
        return df
    
    def rename_exb_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                'exb_sku_sc_rub': "sku_sc_rub",
                'exb_cnt': "cnt",
                'exb_sum_sc_rub': "sc_rub",
                'exb_sum_sc_euro': "sc_euro",
            }
        )
        
        return df

    def calc_sku_sc_euro(df: pd.DataFrame) -> pd.DataFrame:
        df['sku_sc_euro'] = [
            round(euro / cnt, 2)
            for euro, cnt in zip(
                df['sc_euro'],
                df['cnt']
            )
        ]
        
        return df

    df_expend_balance: pd.DataFrame = (
        df_source
        [
            ['date', 'day', 'week', 'month', 'quarter', 'year']
            + HEADERS[:-16]
            + HEADERS[-8:-4]
        ]
    )

    df_expend_balance = (
        df_expend_balance
        .pipe(func=filldown_in_dm)
        .pipe(func=filldown_in_db)
        .pipe(func=filldown_in_ca)
        .pipe(func=dropna_in_sku_id)
        .pipe(func=drop_exb_cnt_nan)
        .pipe(func=drop_exb_cnt_below_zero)
        .pipe(func=fillna_in_exb_digit_cols)
        .pipe(func=rename_exb_columns)
        .pipe(func=calc_sku_sc_euro)
    )

    # Сохранение данных в базу
    for doc_type in df_expend_balance['dm_type'].unique():
        with engine.connect() as connection:
            # if CheckExistsDataInTable().batch_movement_by_date(report_date):
            #     logger.info(f'Данные за {report_date} есть в базе')
                
            # else:
            (
                df_expend_balance[df_expend_balance['dm_type'] == doc_type]
                .to_sql(
                    name=f'bm_ex_{doc_type}',
                    con=connection,
                    schema='report',
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )
            )
            logger.info(f'Документ: "{doc_type}" сохранён')
    
    
    # ##################################################
    # КОНЕЧНЫЙ ОСТАТОК
    # ##################################################

    def fill_miss_val_enb_dm_type(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'dm_type'] = 'final'
        
        return df

    def drop_enb_cnt_nan(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset='enb_cnt')
        
        return df

    def drop_enb_cnt_below_zero(df: pd.DataFrame) -> pd.DataFrame:
        df = df.query('enb_cnt > 0')
        
        return df

    def fillna_in_enb_digit_cols(df: pd.DataFrame) -> pd.DataFrame:
        
        for column_name in ('enb_sku_sc_rub', 'enb_sum_sc_rub', 'enb_sum_sc_euro'):
            df.loc[:, column_name] = df[column_name].fillna(0.0)
        
        return df
    
    def rename_enb_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                'enb_sku_sc_rub': "sku_sc_rub",
                'enb_cnt': "cnt",
                'enb_sum_sc_rub': "sc_rub",
                'enb_sum_sc_euro': "sc_euro",
            }
        )
        
        return df
    
    def calc_sku_sc_euro(df: pd.DataFrame) -> pd.DataFrame:
        df['sku_sc_euro'] = [
            round(euro / cnt, 2)
            for euro, cnt in zip(
                df['sc_euro'],
                df['cnt']
            )
        ]
        
        return df

    df_end_balance: pd.DataFrame = (
        df_source
        [
            ['date', 'day', 'week', 'month', 'quarter', 'year']
            + HEADERS[:-16]
            + HEADERS[-4:]
        ]
    )

    df_end_balance = (
        df_end_balance
        .pipe(func=fill_miss_val_in_dm_date)
        .pipe(func=fill_miss_val_enb_dm_type)
        .pipe(func=filldown_in_dm)
        .pipe(func=filldown_in_db)
        .pipe(func=filldown_in_ca)
        .pipe(func=dropna_in_sku_id)
        .pipe(func=drop_enb_cnt_nan)
        .pipe(func=drop_enb_cnt_below_zero)
        .pipe(func=fillna_in_enb_digit_cols)
        .pipe(func=rename_enb_columns)
        .pipe(func=calc_sku_sc_euro)
    )

    with engine.connect() as connection:
    #     if CheckExistsDataInTable().batch_movement_by_date(prefix='en', doc_typ='final', report_date=report_date):
    #         logger.info(f'Данные за {report_date} есть в базе')
            
    # else:
        df_end_balance.to_sql(
            name='bm_en_final',
            con=connection,
            schema='report',
            if_exists='append',
            index=False,
            chunksize=10_000
        )
    
    
    # ##################################################
    # ТОВАРЫ В ПУТИ ОСТАТОК
    # ##################################################
        
    def fill_miss_val_trb_dm_type(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'dm_type'] = 'transfer'
        
        return df

    def drop_trb_cnt_nan(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset='enb_cnt')
        
        return df

    def drop_trb_cnt_below_zero(df: pd.DataFrame) -> pd.DataFrame:
        df = df.query('enb_cnt > 0')
        
        return df

    def fillna_in_trb_digit_cols(df: pd.DataFrame) -> pd.DataFrame:
        
        for column_name in ('enb_sku_sc_rub', 'enb_sum_sc_rub', 'enb_sum_sc_euro'):
            df.loc[:, column_name] = df[column_name].fillna(0.0)
        
        return df

    def rename_digit_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "enb_sku_sc_rub": "sku_sc_rub",
                "enb_cnt": "cnt",
                "enb_sum_sc_rub": "sc_rub",
                "enb_sum_sc_euro": "sc_euro"
            }
        )
        
        return df
    
    def calc_sku_sc_euro(df: pd.DataFrame) -> pd.DataFrame:
        df['sku_sc_euro'] = [
            round(euro / cnt, 2)
            for euro, cnt in zip(
                df['sc_euro'],
                df['cnt']
            )
        ]
        
        return df

    df_transfer_balance: pd.DataFrame = (
        df_transfer
        [
            ['date', 'day', 'week', 'month', 'quarter', 'year']
            + HEADERS[:-16]
            + HEADERS[-4:]
        ]
    )

    df_transfer_balance = (
        df_transfer_balance
        .pipe(func=fill_miss_val_in_dm_date)
        .pipe(func=fill_miss_val_trb_dm_type)
        .pipe(func=filldown_in_dm)
        .pipe(func=filldown_in_db)
        .pipe(func=filldown_in_ca)
        .pipe(func=dropna_in_sku_id)
        .pipe(func=drop_trb_cnt_nan)
        .pipe(func=drop_trb_cnt_below_zero)
        .pipe(func=fillna_in_trb_digit_cols)
        .pipe(func=rename_digit_columns)
        .pipe(func=calc_sku_sc_euro)
    )

    with engine.connect() as connection:
    #     if CheckExistsDataInTable().batch_movement_by_date(prefix='tr', doc_type='transfer', report_date=report_date):
    #         logger.info(f'Данные за {report_date} есть в базе')
            
    # else:
        df_transfer_balance.to_sql(
            name='bm_tr_transfer',
            con=connection,
            schema='report',
            if_exists='append',
            index=False,
            chunksize=10_000
        )
