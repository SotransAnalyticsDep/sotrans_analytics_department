import os
import sys
from re import sub
from itertools import combinations

import pandas as pd


sys.path.append(r'C:\Users\user\Desktop\github\sotrans_analytics_department')
from packages.utils.decorators import execution_time


PATH: str = r'C:\Users\user\Desktop\github\sotrans_analytics_department\projects\tools\article_crossing\dev\data\data.csv'


def create_dataframe(path: str) -> pd.DataFrame:
    return pd.read_csv(
        filepath_or_buffer=path,
        sep=',',
        header=None,
        names=['origin_id', 'origin_brand', 'analog_id', 'analog_brand'],
        encoding='utf-8',
        dtype='str'
    )


def check_total_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:
    print(f'Удалено полных дубликатов строк: {dataframe.duplicated().sum()}')
    return dataframe.drop_duplicates()


def chech_analog_duplicates(dataframe: pd.DataFrame):
    duplicated_analogs: pd.DataFrame = dataframe.value_counts(subset=['analog_id', 'analog_brand']).reset_index().query(expr='count > 1')
    duplicated_analogs['KEY'] = duplicated_analogs['analog_id'].str.cat(duplicated_analogs['analog_brand'], sep=' - ')
    temp_df = dataframe.copy()
    temp_df['KEY'] = dataframe['analog_id'].str.cat(dataframe['analog_brand'], sep=' - ')
    temp_df = temp_df[temp_df['KEY'].isin(values=duplicated_analogs['KEY'])].sort_values(by=['KEY', 'origin_id', 'origin_brand', 'analog_id', 'analog_brand'])
    print(f'Количество дубликатов в аналогах: {len(temp_df)}')

    if len(temp_df) > 0:
        print(temp_df, '\n')
        is_continue: bool = True if input('Продолжить выполнение? (y/n)').lower() == 'y' else False
        if is_continue:
            return dataframe
    return dataframe


def clear_punctuation_symbols(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Очистка от символов пунктуации и спецсимволов;
    for column_name in ('origin_id', 'analog_id'):
        dataframe.loc[:, column_name] = [
            sub('[^A-Za-z0-9]+', '', article)
            for article in dataframe.loc[:, column_name]
        ]
    return dataframe


def create_key_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Создани ключ-столбцов;
    for article_column, brand_column, prefix in zip(
            ('origin_id', 'analog_id'),
            ('origin_brand', 'analog_brand'),
            ('ORIGIN', 'ANALOG')
    ):
        dataframe.loc[:, f'{prefix}_KEY'] = [
            f'{article} - {brand}'
            for article, brand in zip(
                dataframe.loc[:, article_column],
                dataframe.loc[:, brand_column]
            )
        ]
    return dataframe


def create_source_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe['source_id_brand'] = dataframe['ORIGIN_KEY']
    return dataframe


@execution_time
def main() -> pd.DataFrame:
    # Конвеер: создание датафрейма и трансформация данных; 
    dataframe: pd.DataFrame = (
        create_dataframe(path=PATH)
        .pipe(func=check_total_duplicates)
        .pipe(func=chech_analog_duplicates)
        .pipe(func=clear_punctuation_symbols)
        .pipe(func=create_key_columns)
        .pipe(func=create_source_column)
    )

    # Получение множства уникальных пар;
    ORIGIN_UNIQUE: set[str] = set(dataframe['ORIGIN_KEY'])

    # Создание пустого датафрейма для итоговых значений;
    result_dataframe: pd.DataFrame = pd.DataFrame(columns=['source_id_brand', 'origin_id', 'origin_brand', 'analog_id', 'analog_brand'])

    # Основной алгоритм;
    for unique_pair in ORIGIN_UNIQUE:

        # Создать временный датафрейм, отфильтрованный по уникальной паре;
        temp_dataframe: pd.DataFrame = dataframe.query(expr='ORIGIN_KEY == @unique_pair')

        # Создание множества из отфильтрованный данных;
        temp_unique_pairs: set[str] = set(temp_dataframe['ANALOG_KEY'])
        if len(temp_unique_pairs) == 1:
            break

        def _create_temp_dataframe(unique_pairs: set[str]) -> pd.DataFrame:
            return pd.DataFrame(data=combinations(unique_pairs, 2))

        def _explode_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
            for new_column_names, old_column_name in zip(
                    (['origin_id', 'origin_brand'], ['analog_id', 'analog_brand']),
                    (0, 1)
            ):
                dataframe[new_column_names] = dataframe[old_column_name].str.split(pat=' - ', n=1, expand=True)
            return dataframe

        def _drop_source_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
            return dataframe.drop(labels=[0, 1], axis=1)

        def _create_source_id_brand_column(dataframe: pd.DataFrame, value: str) -> pd.DataFrame:
            dataframe['source_id_brand'] = value
            return dataframe

        temp_result_dataframe: pd.DataFrame = (
            # Формирование промежуточного датафрейма;
            _create_temp_dataframe(unique_pairs=temp_unique_pairs)
            # Разделить значения "артикул - бренд" по разным столбцам;
            .pipe(func=_explode_columns)
            # Удалить исходные столбцы;
            .pipe(func=_drop_source_columns)
            # Создать столбец с номером, через который сформировались кросс-номера;
            .pipe(func=_create_source_id_brand_column, value=unique_pair)
        )

        # Объединение результатов;
        result_dataframe = pd.concat(
            objs=[result_dataframe, temp_result_dataframe]
        )

    (
        pd.concat(
            objs=[
                dataframe.drop(labels=['ORIGIN_KEY', 'ANALOG_KEY'], axis=1),
                result_dataframe
            ],
            ignore_index=True
        )
        .drop_duplicates()
        .to_excel(
            excel_writer=os.path.join(PATH.rsplit('\\', maxsplit=1)[0], 'result.xlsx'),
            engine='openpyxl',
            index=False
        )
    )


if __name__ == '__main__':
    main()
