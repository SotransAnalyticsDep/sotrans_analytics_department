"""
Модуль предоставляет методы формирования SQL-запросов к базе данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import Optional, Tuple

from loguru import logger

from ..utils._sql_builder import build_sql_query_by_date


# ##################################################
# КЛАССЫ
# ##################################################
class SQLArrival:
    """
    Класс предоставляет методы формирования SQL-запросов к базе данных.
    """

    # ##################################################
    # API
    # ##################################################
    @staticmethod
    def get_sql_query_arrival(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
        agg_func: str,
    ) -> Optional[str]:


        # Формирование SQL-запроса
        query: str = build_sql_query_by_date(
            prefix=prefix,
            doc_type=doc_type,
            agg_cat_cols=agg_cat_cols,
            agg_dgt_cols=agg_dgt_cols,
        )
        logger.success("SQL-запрос успешно сформирован")
        return query
