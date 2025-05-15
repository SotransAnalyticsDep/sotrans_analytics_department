"""
Модуль предоставляет методы формирования SQL-запросов к базе данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import Optional, Tuple

from loguru import logger

from ..utils._sql_builder import (
    build_sql_query_with_distinct
)


# ##################################################
# КЛАССЫ
# ##################################################
class SQLHaving:
    """
    Класс предоставляет методы формирования SQL-запросов к базе данных.
    """

    # ##################################################
    # API
    # ##################################################
    @staticmethod
    def get_sql_query_stock(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        #agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
    ) -> Optional[str]:

        # Формирование SQL-запроса
        query: str = build_sql_query_with_distinct(
            prefix=prefix,
            doc_type=doc_type,
            period=period,
            agg_cat_cols=agg_cat_cols,
            agg_dt_cols=agg_dt_cols,
        )
        logger.success("SQL-запрос успешно сформирован")
        return query
