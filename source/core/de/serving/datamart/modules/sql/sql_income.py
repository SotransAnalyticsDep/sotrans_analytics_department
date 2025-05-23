"""
Модуль предоставляет методы формирования SQL-запросов к базе данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import Optional, Tuple

from loguru import logger

from ..config.config import DEFAULT_VALID_AGG_FUNCS
from ..utils._sql_builder import (
    build_sql_query_by_date,
    build_sql_query_with_pre_agg_data,
)


# ##################################################
# КЛАССЫ
# ##################################################
class SQLIncome:
    """
    Класс предоставляет методы формирования SQL-запросов к базе данных.
    """

    # ##################################################
    # API
    # ##################################################
    @staticmethod
    def get_sql_query_complect(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_decomplect(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_entering(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_inventory(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_movement(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_receipt(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_resort(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")


    @staticmethod
    def get_sql_query_update(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> str:

        if agg_func == "first":
            # Формирование SQL-запроса
            query: str = build_sql_query_by_date(
                prefix=prefix,
                doc_type=doc_type,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
            )
            logger.success("SQL-запрос успешно сформирован")

            return query

        elif agg_func in DEFAULT_VALID_AGG_FUNCS:
            # Формирование SQL-запроса
            query: str = build_sql_query_with_pre_agg_data(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                agg_cat_cols=agg_cat_cols,
                agg_dgt_cols=agg_dgt_cols,
                agg_dt_cols=agg_dt_cols,
                agg_func=agg_func,
            )
            logger.success("SQL-запрос успешно сформирован")
            return query
        else:
            raise ValueError("Неизвестная функция агрегации")