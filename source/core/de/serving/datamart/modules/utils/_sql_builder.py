"""
Модуль предоставляет методы формирования SQL-запросов.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import List, Tuple

from loguru import logger


# ##################################################
# ФУНКЦИИ
# ##################################################
def build_sql_query_by_date(
    prefix: str,
    doc_type: str,
    agg_cat_cols: Tuple[str, ...],
    agg_dgt_cols: Tuple[str, ...],
) -> str:
    """
    Метод формирует SQL-запрос на основании входных данных для получения данных
    на начальную дату анализируемого периода.

    Args:
        prefix(str): Префикс типа документа.
            Доступны: st, in, hv, ex, en, tr, sl, ld.
        doc_type(str): Тип документа (хоз. операция).
            Например: init, entering, inventory, update.
        agg_cat_cols(Tuple[str]): Кортеж с категориальными столбцами.
        agg_dgt_cols(Tuple[str]): Кортеж с временными столбцами.

    Returns:
        str: Сформированный SQL-запрос.
    """

    # Формирование блока SELECT
    logger.trace("Формирование блока SELECT")
    slct_cat_cols: List[str] = [f"{col} AS {col}" for col in agg_cat_cols]

    # Формирование блока агрегации
    logger.trace("Формирование блока агрегации")
    slct_dgt_cols: List[str] = [
        f"round(sum({col})::decimal, 2) AS {prefix}_{doc_type}_{col}_fd"
        for col in agg_dgt_cols
    ]

    # Формирование блока GROUP BY
    logger.trace("Формирование блока GROUP BY")
    gb_cat_cols: str = ", ".join([col for col in agg_cat_cols])

    # Сборка итогового SQL-запроса
    logger.trace("Сборка итогового SQL-запроса")
    query: str = f"""
    SELECT {", ".join(slct_cat_cols + slct_dgt_cols)}
    FROM report.vw_{prefix}_{doc_type}
    WHERE date = %s
    GROUP BY {gb_cat_cols}
    """

    return query


def build_sql_query_with_pre_agg_data(
    prefix: str,
    doc_type: str,
    period: int,
    agg_cat_cols: Tuple[str, ...],
    agg_dgt_cols: Tuple[str, ...],
    agg_dt_cols: Tuple[str, ...],
    agg_func: str,
) -> str:
    """
    Метод формирует SQL-запрос на основании входных данных для получения
    агрегированного результата за анализируемый диапазон.

    Args:
        prefix(str): Префикс типа документа.
            Доступны: st, in, hv, ex, en, tr, sl, ld.
        doc_type(str): Тип документа (хоз. операция).
            Например: init, entering, inventory, update.
        period(int): Анализируемый диапазон в количестве месяцев.
        agg_cat_cols: Кортеж с категориальными столбцами.
        agg_dgt_cols: Кортеж с числовыми столбцами.
        agg_dt_cols: Кортеж с временными столбцами.
        agg_func: Функция агрегации.

    Returns:
        (str): SQL-запрос.
    """

    # Формирование блока SELECT для CTE
    cte_slct_dt_cols: List[str] = [f"{col} AS {col}" for col in agg_dt_cols]
    cte_slct_cat_cols: List[str] = [f"{col} AS {col}" for col in agg_cat_cols]
    cte_slct_dgt_cols: List[str] = [
        f"round(sum({col})::decimal, 2) AS {prefix}_{doc_type}_{col}"
        for col in agg_dgt_cols
    ]

    # Формирование блока GROUP BY для CTE
    cte_gb: str = ", ".join(agg_dt_cols + agg_cat_cols)

    # Формирование блока SELECT основного запроса
    slct_cat_cols: List[str] = [f"{col} AS {col}" for col in agg_cat_cols]
    slct_dgt_cols: List[str] = [
        (
            f"round({agg_func}({prefix}_{doc_type}_{col})::decimal, 2) AS "
            f"{prefix}_{doc_type}_{col}_{agg_func}_{period}_{agg_dt_cols[-1]}"
        )
        for col in agg_dgt_cols
    ]

    # Формирование блока GROUP BY основного запроса
    gb: str = ", ".join(agg_cat_cols)

    # Сборка полного SQL-запроса с использованием параметризации дат
    query: str = f"""
    WITH agg_data AS (
        SELECT {", ".join(cte_slct_dt_cols + cte_slct_cat_cols + cte_slct_dgt_cols)}
        FROM report.vw_{prefix}_{doc_type}
        WHERE date BETWEEN %s AND %s
        GROUP BY {cte_gb}
    )
    SELECT {", ".join(slct_cat_cols + slct_dgt_cols)}
    FROM agg_data
    GROUP BY {gb}
    """

    return query


def build_sql_query_with_distinct(
        prefix: str,
        doc_type: str,
        period: int,
        agg_cat_cols: Tuple[str, ...],
        agg_dgt_cols: Tuple[str, ...],
        agg_dt_cols: Tuple[str, ...],
) -> str:
    """
    Метод формирует SQL-запрос на основании входных данных для получения данных
    на начальную дату анализируемого периода.

    Args:
        prefix(str): Префикс типа документа.
            Доступны: st, in, hv, ex, en, tr, sl, ld.
        doc_type(str): Тип документа (хоз. операция).
            Например: init, entering, inventory, update.
        agg_cat_cols(Tuple[str]): Кортеж с категориальными столбцами.
        agg_dgt_cols(Tuple[str]): Кортеж с временными столбцами.

    Returns:
        str: Сформированный SQL-запрос.
    """

    # Формирование блока SELECT
    logger.trace("Формирование блока SELECT")
    slct_cat_cols: List[str] = [f"{col} AS {col}" for col in agg_cat_cols]

    # Формирование блока агрегации
    logger.trace("Формирование блока агрегации")
    slct_dgt_cols: List[str] = [
        f"count(distinct({agg_dt_cols[-1]})) AS {prefix}_{doc_type}_{period}_{agg_dt_cols[-1]}"
    ]

    # Формирование блока GROUP BY
    logger.trace("Формирование блока GROUP BY")
    gb_cat_cols: str = ", ".join([col for col in agg_cat_cols])

    # Сборка итогового SQL-запроса
    logger.trace("Сборка итогового SQL-запроса")
    query: str = f"""
    SELECT {", ".join(slct_cat_cols + slct_dgt_cols)}
    FROM report.vw_{prefix}_{doc_type}
    WHERE date between %s and %s
    GROUP BY {gb_cat_cols}
    """

    return query
