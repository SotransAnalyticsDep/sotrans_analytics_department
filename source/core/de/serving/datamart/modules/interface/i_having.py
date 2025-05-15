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
from ..utils import execute_sql_query, is_cols_available, merge_df_with_dm
from . import IBasic


# ##################################################
# КЛАССЫ
# ##################################################
class IHaving(IBasic):
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
    def add_stock(
        self,
        period: Optional[int] = None,
        start_date: Optional[dt.date] = None,
        end_date: Optional[dt.date] = None,
        agg_cat_cols: Optional[Tuple[str, ...]] = None,
        agg_dgt_cols: Optional[Tuple[str, ...]] = None,
        agg_dt_cols: Optional[Tuple[str, ...]] = None,
        agg_func: Optional[str] = "count",
        how_to_merge: str = "outer"
    ) -> None:

        # Валидация данных
        if end_date is None:
            end_date: dt.date = self.__datamart.end_date
        if period is None:
            period: int = self.__datamart.period
            start_date: dt.date = self.__datamart.start_date
        else:
            if period <= 0:
                err_msg: str = "Период должен быть положительным числом"
                logger.error(err_msg)
                raise ValueError(err_msg)
            else:
                start_date: dt.date = self.__datamart.calc_start_date(
                    period=period - 1,
                    end_date=end_date
                )
        if agg_cat_cols is None:
            agg_cat_cols: Tuple[str, ...] = self.__datamart.agg_cat_cols
        if agg_dgt_cols is None:
            agg_dgt_cols: Tuple[str, ...] = self.__datamart.agg_dgt_cols
        if agg_dt_cols is None:
            agg_dt_cols: Tuple[str, ...] = self.__datamart.agg_dt_cols
        dt_col: str = agg_dt_cols[-1]

        # Проверка доступности числовых столбцов
        available_dgt_cols: Optional[Tuple[str, ...]] = is_cols_available(
            prefix="hv",
            doc_type="stock",
            period=period,
            agg_dgt_cols=agg_dgt_cols,
            agg_func=agg_func,
            dt_col=dt_col,
            exists_cols=tuple(self.__datamart.df.columns)
        )

        if not available_dgt_cols:
            logger.warning("Нет доступных колонок для операции")
            return None

        # Получение метода формирования SQL-запроса
        sql_method = METHODS_MAP["hv"]["stock"]

        # Получение SQL-запроса
        sql_query: str = sql_method(
            prefix="hv",
            doc_type="stock",
            period=period,
            agg_cat_cols=agg_cat_cols,
            agg_dt_cols=agg_dt_cols,
        )

        # Выполнение SQL-запроса
        dataframe: pd.DataFrame = execute_sql_query(
            engine=self.__datamart.engine,
            query=sql_query,
            params=(str(start_date), str(end_date)),
        )

        # Добавление данных из DataFrame к витрине данных
        merge_df_with_dm(
            dm=self.__datamart,
            df=dataframe,
            agg_cat_cols=agg_cat_cols,
            how_to_merge=how_to_merge
        )


    def add_sale(
        self,
        period: Optional[int] = None,
        start_date: Optional[dt.date] = None,
        end_date: Optional[dt.date] = None,
        agg_dt_cols: Optional[Tuple[str, ...]] = None,
        agg_func: Optional[str] = "count",
        how_to_merge: str = "outer"
    ) -> None:

        # Валидация данных
        if end_date is None:
            end_date: dt.date = self.__datamart.end_date
        if period is None:
            period: int = self.__datamart.period
            start_date: dt.date = self.__datamart.start_date
        else:
            if period <= 0:
                err_msg: str = "Период должен быть положительным числом"
                logger.error(err_msg)
                raise ValueError(err_msg)
            else:
                start_date: dt.date = self.__datamart.calc_start_date(
                    period=period - 1,
                    end_date=end_date
                )

        if agg_dt_cols is None:
            agg_dt_cols: Tuple[str, ...] = self.__datamart.agg_dt_cols

        # Получение метода формирования SQL-запроса
        sql_method = METHODS_MAP["hv"]["sale"]

        # Получение SQL-запроса
        sql_query: str = sql_method(
            prefix="hv",
            doc_type="sale",
            period=period,
            agg_cat_cols=self.__datamart.agg_cat_cols,
            agg_dt_cols=agg_dt_cols,
        )

        # Выполнение SQL-запроса
        dataframe: pd.DataFrame = execute_sql_query(
            engine=self.__datamart.engine,
            query=sql_query,
            params=(str(start_date), str(end_date)),
        )

         # Добавление данных из DataFrame к витрине данных
        merge_df_with_dm(
            dm=self.__datamart,
            df=dataframe,
            how_to_merge=how_to_merge
        )
