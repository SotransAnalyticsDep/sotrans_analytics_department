"""
ETL-процесс отвечает за обновление данных по кросс-номерам в базе данных.
"""
# ##################################################
# IMPORTS
# ##################################################
import sys
import pandas as pd
from sqlalchemy import Engine, text
from loguru import logger

sys.path.append(r'C:\Users\user\Desktop\github\new')
from source.database import PGConnector


# ##################################################
# CLASSES
# ##################################################
class ETLCrossNumbers:
    """
    Класс для обновления данных по кросс-номерам в базе данных.
    """


    def __init__(self) -> None:
        """
        Конструктор класса для обновления данных по кросс-номерам в базе данных.
        """
        self.__engine: Engine = PGConnector().engine
        self.__trade_path: str = r'\\192.168.101.228\Trade\analytics_department'
        self.__cross_numbers_file: str = rf'{self.__trade_path}\lookup\cross_numbers\sotrans_cross_numbers.json'


    # ##################################################
    # PRIVATE METHODS
    # ##################################################
    def __extract(self) -> pd.DataFrame:
        """
        Метод для извлечения данных по кросс-номерам из файла.
        
        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам.
        """
        dataframe: pd.DataFrame = pd.read_json(
            path_or_buf=self.__cross_numbers_file,
            orient='records',
            encoding='utf-8-sig'
        )
        logger.success('Извлечение данных по кросс-номерам завершено успешно.')
        
        return dataframe


    def __change_reg_to_lower(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для преобразования регистра в нижний для данных по кросс-номерам.
        
        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.
        
        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам в нижнем регистре.
        """
        for col in dataframe.columns:
            dataframe.loc[:, col] = dataframe.loc[:, col].str.lower().str.strip()
        logger.success('Преобразование регистра в нижний для данных по кросс-номерам завершено успешно.')
        
        return dataframe


    def __create_ab_combination(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для создания комбинации A-B для данных по кросс-номерам.
        
        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.
        
        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам с комбинацией A-B.
        """
        dataframe: pd.DataFrame = (
            pd.DataFrame()
            .assign(
                brand_name=dataframe['brand_name'],
                sku_art_num=dataframe['product_catalog_number'],
                brand_name_analog=dataframe['brand_name_analog'],
                sku_art_num_analog=dataframe['product_catalog_number_analog']
            )
        )
        logger.success('Создание комбинации A-B для данных по кросс-номерам завершено успешно.')
        
        return dataframe


    def __create_aa_combination(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для создания комбинации A-A для данных по кросс-номерам.
        
        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.
        
        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам с комбинацией A-A.
        """
        dataframe: pd.DataFrame = (
            pd.DataFrame()
            .assign(
                brand_name=dataframe['brand_name'],
                sku_art_num=dataframe['product_catalog_number'],
                brand_name_analog=dataframe['brand_name'],
                sku_art_num_analog=dataframe['product_catalog_number']
            )
        )
        logger.success('Создание комбинации A-A для данных по кросс-номерам завершено успешно.')
        
        return dataframe


    def __create_ba_combination(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для создания комбинации B-A для данных по кросс-номерам.
        
        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.
        
        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам с комбинацией B-A.
        """
        dataframe: pd.DataFrame = (
            pd.DataFrame()
            .assign(
                brand_name=dataframe['brand_name_analog'],
                sku_art_num=dataframe['product_catalog_number_analog'],
                brand_name_analog=dataframe['brand_name'],
                sku_art_num_analog=dataframe['product_catalog_number']
            )
        )
        logger.success('Создание комбинации B-A для данных по кросс-номерам завершено успешно.')
        
        return dataframe


    def __create_bb_combination(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для создания комбинации B-B для данных по кросс-номерам.

        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.

        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам с комбинацией B-B.
        """

        dataframe: pd.DataFrame = (
            pd.DataFrame()
            .assign(
                brand_name=dataframe['brand_name_analog'],
                sku_art_num=dataframe['product_catalog_number_analog'],
                brand_name_analog=dataframe['brand_name_analog'],
                sku_art_num_analog=dataframe['product_catalog_number_analog']
            )
        )
        logger.success('Создание комбинации B-B для данных по кросс-номерам завершено успешно.')

        return dataframe


    def __concat_combination(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для объединения комбинаций для данных по кросс-номерам.

        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам с объединенными комбинациями.
        """

        dataframe: pd.DataFrame = (
            pd.concat(
                objs=[
                    self.__create_ab_combination(dataframe=dataframe),
                    self.__create_aa_combination(dataframe=dataframe),
                    self.__create_ba_combination(dataframe=dataframe),
                    self.__create_bb_combination(dataframe=dataframe)
                ]
            )
        )
        logger.success('Объединение комбинаций для данных по кросс-номерам завершено успешно.')

        return dataframe


    def __drop_empty_values(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для удаления пустых значений для данных по кросс-номерам.

        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.

        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам без пустых значений.
        """

        dataframe: pd.DataFrame = (
            dataframe
            [dataframe['brand_name'] != '']
        )
        logger.success('Удаление пустых значений для данных по кросс-номерам завершено успешно.')

        return dataframe


    def __sort_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для сортировки данных по кросс-номерам.

        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.

        Returns:
            pd.DataFrame: Отсортированный датафрейм с данными по кросс-номерам.
        """

        dataframe: pd.DataFrame = (
            dataframe
            .sort_values(
                by=[
                    'brand_name',
                    'sku_art_num',
                    'brand_name_analog',
                    'sku_art_num_analog'
                ]
            )
        )
        logger.success('Сортировка данных по кросс-номерам завершена успешно.')

        return dataframe


    def __drop_duplicates(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Метод для удаления дубликатов в данных по кросс-номерам.

        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.

        Returns:
            pd.DataFrame: Датафрейм с данными по кросс-номерам без дубликатов.
        """

        dataframe: pd.DataFrame = (
            dataframe
            .drop_duplicates(ignore_index=True)
        )
        logger.success('Удаление дубликатов в данных по кросс-номерам завершено успешно.')

        return dataframe


    def __load_data(self, dataframe: pd.DataFrame) -> None:
        """
        Метод для загрузки данных по кросс-номерам в базу данных.
        
        Args:
            dataframe (pd.DataFrame): Датафрейм с данными по кросс-номерам.
        """

        with self.__engine.begin() as connection:
            connection.execute(text('TRUNCATE TABLE constant.cross_numbers'))

            dataframe.to_sql(
                name='cross_numbers',
                schema='constant',
                con=connection,
                if_exists='append',
                index=False,
                chunksize=10_000
            )
        logger.success('Загрузка данных по кросс-номерам в базу данных завершена успешно.')


    # ##################################################
    # PUBLIC METHODS
    # ##################################################
    def run(self) -> None:
        """
        Метод для выполнения процесса создания комбинаций данных по кросс-номерам.
        """

        (
            # 1. Получение данных по кросс-номерам из файла JSON;
            self.__extract()
            # 2. Приведение строк к нижнему регистру;
            .pipe(func=self.__change_reg_to_lower)
            # 3. Создание комбинаций кросс-номеров и объединение в один DataFrame;
            .pipe(func=self.__concat_combination)
            # 4. Удаление пустых значений;
            .pipe(func=self.__drop_empty_values)
            # 5. Сортировка данных;
            .pipe(func=self.__sort_dataframe)
            # 6. Удаление дубликатов;
            .pipe(func=self.__drop_duplicates)
            # 7. Очистка таблицы и загрузка новых данных в базу данных;
            .pipe(func=self.__load_data)
        )
