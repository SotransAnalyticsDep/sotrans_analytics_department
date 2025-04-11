"""
Модуль предоставляет интерфейс взаимодействия с данными "Начальный Остаток".
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
import datetime as dt
from typing import Optional, Tuple

import pandas as pd
from loguru import logger

from ..utils import METHODS_MAP
from ..utils import execute_sql_query, merge_df_with_dm
from . import IBasic


# ##################################################
# КЛАССЫ
# ##################################################
class IOrder(IBasic):
    """
    Интерфейс для манипуляций с данными "Начального Остатка"
    """

    def __init__(self, datamart) -> None:
        self.__datamart = datamart

    def process(self) -> None:
        logger.info("Обработка 'Начального Остатка'")

    # ##################################################
    # API
    # ##################################################
    def add_order(
        self,
        agg_cat_cols: Optional[Tuple[str, ...]] = None,
        how_to_merge: str = 'outer'
    ) -> None:
        
        if agg_cat_cols is None:
            agg_cat_cols = self.__datamart.agg_cat_cols

        # Проверка доступности числовых столбцов
        available_dgt_cols: Optional[Tuple[str, ...]] = [
            col for col in ('backorder_cnt', 'shipped_cnt')
            if col not in self.__datamart.df.columns
        ]

        if not available_dgt_cols:
            logger.warning("Нет доступных колонок для операции")
            return None

        # Получение метода формирования SQL-запроса
        sql_method = METHODS_MAP["cr"]["orders"]


        # Получение SQL-запроса
        sql_query: str = sql_method(
            prefix='cr',
            doc_type='orders',
            agg_cat_cols=agg_cat_cols,
        )

        # Выполнение SQL-запроса
        dataframe: pd.DataFrame = execute_sql_query(
            engine=self.__datamart.engine,
            query=sql_query,
        )

         # Добавление данных из DataFrame к витрине данных
        merge_df_with_dm(
            dm=self.__datamart,
            df=dataframe,
            how_to_merge=how_to_merge
        )