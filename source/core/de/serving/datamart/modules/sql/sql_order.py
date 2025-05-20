"""
Модуль предоставляет методы формирования SQL-запросов к базе данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import Tuple

from loguru import logger

from ..utils._sql_builder import (
    build_sql_query_order,
)


# ##################################################
# КЛАССЫ
# ##################################################
class SQLOrder:
    """
    Класс предоставляет методы формирования SQL-запросов к базе данных.
    """

    # ##################################################
    # API
    # ##################################################
    @staticmethod
    def get_sql_query_order(
            prefix: str,
            doc_type: str,
            agg_cat_cols: Tuple[str]
    ) -> str:
    
        # Формирование SQL-запроса
        query: str = build_sql_query_order(
            prefix=prefix,
            doc_type=doc_type,
            agg_cat_cols=agg_cat_cols,
        )
        logger.success("SQL-запрос успешно сформирован")
        
        return query


