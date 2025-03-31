"""
Модуль отвечает за валидацию входных данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from functools import lru_cache
from typing import Tuple

from loguru import logger

from ..config.config import DEFAULT_VALID_AGG_FUNCS


# ##################################################
# ЗАЩИЩЁННЫЕ ФУНКЦИИ
# ##################################################
@lru_cache(maxsize=16)
def __gen_col_name(
    prefix: str,
    doc_type: str,
    period: int,
    dgt_col: str,
    agg_func: str,
    dt_col: str,
) -> str:
    """
    Метод формирует наименование столбца в зависимости от функции агрегации.

    Args:
        prefix (str): Префикс типа документа.
            Доступны: st, in, hv, ex, en, tr, sl, ld.
        doc_type (str): Тип документа (хоз. операция).
            Например: init, entering, inventory, update.
        dgt_col (str): Наименование исходного числового столбца для
            формирования нового.
        agg_func (str): Функция агрегации.
        period (int): Анализируемый период в количестве месяцев.
        dt_col (str): Наименование типа даты, за которе производилась агрегация.

    Returns:
        str: Строка с новым наименованием столбца.
    """

    if prefix == "hv":
        return f"{prefix}_{doc_type}_{period}_{dt_col}"

    if agg_func == "first":
        return f"{prefix}_{doc_type}_{dgt_col}_fd"
    elif agg_func == "last":
        return f"{prefix}_{doc_type}_{dgt_col}_ld"
    return f"{prefix}_{doc_type}_{dgt_col}_{agg_func}_{period}_{dt_col}"


# ##################################################
# API
# ##################################################
def is_cols_available(
    prefix: str,
    doc_type: str,
    period: int,
    agg_dgt_cols: Tuple[str, ...],
    dt_col: str,
    agg_func: str,
    exists_cols: Tuple[str, ...],
) -> Tuple[str, ...]:

    if agg_func not in ("first", "last") + DEFAULT_VALID_AGG_FUNCS:
        err_msg: str = f"Некорректная функция агрегации: {agg_func}"
        logger.error(err_msg)
        raise ValueError(err_msg)
    else:
        available_dgt_cols: Tuple[str, ...] = tuple(
            col
            for col in agg_dgt_cols
            if __gen_col_name(
                prefix=prefix,
                doc_type=doc_type,
                period=period,
                dgt_col=col,
                agg_func=agg_func,
                dt_col=dt_col,
            )
            not in exists_cols
        )
        logger.success(f"Кортеж доступных числовых столбцов: {available_dgt_cols}")

        return available_dgt_cols
