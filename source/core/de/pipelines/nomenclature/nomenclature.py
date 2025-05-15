"""
ETL-процесс отвечает за обновление данных в справочнике номенклатуры.
"""

# ##################################################
# IMPORTS
# ##################################################
import sys

import pandas as pd
from sqlalchemy import Engine

sys.path.append(r'C:\Users\user\Desktop\github\new')
from source.database import PGConnector
from source.exception import error_xl_shared_strings_xml


# ##################################################
# CLASSES
# ##################################################
class ETLNomenclature:
    """Класс отвечает за обновление данных в справочнике номенклатуры."""
    
    def __init__(self) -> None:
        self.__filepath: str = r"C:\Users\user\YandexDisk\batch_movement\reference\nomenclature.xlsx"
        self.__pg_connector: Engine = PGConnector().engine
        
        


    # ##################################################
    # PROTECTED METHODS
    # ##################################################
    @error_xl_shared_strings_xml
    def __create_dataframe(self) -> pd.DataFrame:
        dataframe: pd.DataFrame = pd.read_excel(
            io=self.__filepath,
            engine='openpyxl'
        )
        
        return dataframe


    def __drop_na(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe: pd.DataFrame = dataframe.dropna(subset='sku_id_1c', ignore_index=True)
        
        return dataframe


    def __fill_na(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe.loc[dataframe[dataframe['brand_id_1c'].isna()].index, 'brand_id_1c'] = "_нет данных"
        dataframe.loc[dataframe[dataframe['brand_name_1c'].isna()].index, 'brand_name_1c'] = "_нет данных"
        
        return dataframe
    
    
    def __change_registry(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for col in dataframe.columns:
            dataframe.loc[:, col] = [
                str(value).lower().strip()
                for value in dataframe.loc[:, col]
            ]
        
        return dataframe
    
    
    def __get_current_data_from_pg(self) -> pd.DataFrame:
        
        sql_query: str = """
        SELECT
            *
        FROM
            constant.nomenclature
        """

        with self.__pg_connector.begin() as connection:
            pg_nomenclature: pd.DataFrame = pd.read_sql(sql=sql_query, con=connection)

        return pg_nomenclature
    
    
    def __filter_new_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        pg_nomenclature: pd.DataFrame = self.__get_current_data_from_pg()
        
        dataframe: pd.DataFrame = dataframe[~dataframe['sku_id_1c'].isin(pg_nomenclature['sku_id_1c'])]
        
        return dataframe
    
    
    def __add_new_data_to_pg(self, dataframe: pd.DataFrame) -> None:
        with self.__pg_connector.begin() as connection:
            dataframe.to_sql(
                name='nomenclature',
                con=connection,
                if_exists='append',
                index=False,
                schema='constant',
                chunksize=10_000
            )


    # ##################################################
    # PUBLIC METHODS
    # ##################################################
    def run(self) -> None:
        
        (
            self.__create_dataframe()
            .pipe(self.__drop_na)
            .pipe(self.__fill_na)
            .pipe(self.__change_registry)
            .pipe(self.__filter_new_data)
            .pipe(self.__add_new_data_to_pg)
        )
