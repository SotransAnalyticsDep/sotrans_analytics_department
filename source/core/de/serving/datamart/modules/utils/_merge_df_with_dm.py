"""
Модуль предоставляет методы объединения данных, полученных в результате 
SQL-запроса с текущей витриной данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import Tuple, Optional

import pandas as pd
from loguru import logger


# ##################################################
# ФУНКЦИИ
# ##################################################
def merge_df_with_dm(
        dm,
        df: pd.DataFrame,
        agg_cat_cols: Optional[Tuple[str, ...]] = None,
        how_to_merge: str = 'outer'
) -> None:
    """
    Объединяет DataFrame, сформированный на основании SQL-запроса с DataFrame
    из витрины данных.
    
    Args:
        dm(Datamart): Объект витрины данных.
        df(DataFrame): DataFrame, данные из которого необходимо добавить к
        витрине денных.
        how_to_merge(Tuple[str] | str): Кортеж или строка с наименованиями
        столбцов, по которым необходимо производить объединение таблиц.
    
    Raises:
        ValueError: Если объединение невозможно из-за несоответствия колонок.
        Exception: При непредвиденной ошибке во время объединения.
    """
    
    # Выполнение объединения данных, полученных в результате SQL-запроса с
    # текущей витриной данных
    try:
        logger.trace(
            "Объединение DataFrame, сформированного на основании SQL-запроса, "
            "и витрины данных"
        )
        
        if agg_cat_cols is None:
            agg_cat_cols: Tuple[str, ...] = dm.agg_cat_cols
        
        dm.df = dm.df.merge(
            right=df,
            how=how_to_merge,
            on=agg_cat_cols,
        )
    
    except ValueError as ve:
        logger.error(f"Ошибка объединения: несоответствие колонок - {str(ve)}")
        raise
        
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при объединении данных: {str(e)}")
        raise
