"""
Модуль предоставляет методы выполнения SQL-запросов.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from typing import Optional, Tuple
from functools import lru_cache

import pandas as pd
from loguru import logger
from sqlalchemy import Engine


# ##################################################
# ФУНКЦИИ
# ##################################################
@lru_cache(maxsize=32)
def execute_sql_query(
    engine: Engine,
    query: str,
    params: Tuple[str, ...] = None,
) -> Optional[pd.DataFrame]:
    """
    Метод выполняет переданный SQL-запрос, формирует и возвращает DataFrame,
    на основании полученных данных.

    Args:
        engine(Engine): Объект подключения к базе данных.
        query(str): Строка SQL-запроса.
        params(Tuple[str]): Кортеж с параметрами для метода pd.read_sql_query.

    Returns:
        pd.DataFrame: Датафрейм на основании переданного SQL-запроса.
    """

    # Валидация входных данных
    if not query.strip():
        logger.error("Попытка выполнить пустой запрос")
        raise ValueError("SQL-запрос не может быть пустым")

    # Выполнение SQL-запроса с параметрами
    try:
        logger.trace("Установка подключения к базе данных")
        with engine.connect() as connection:
            logger.trace("Подключение к базе данных установлено")
            logger.trace("Выполнение SQL-запроса и формирование DataFrame")
            dataframe: pd.DataFrame = pd.read_sql_query(
                sql=query, con=connection, params=params
            )
            logger.success(
                f"SQL-запрос успешно выполнен. DataFrame сформирован: "
                f"{dataframe.shape}"
            )

            return dataframe

    except Exception as err:
        logger.error(
            f"Непредвиденная ошибка при выполнении запроса или формировании "
            f"DataFrame: {str(err)}"
        )
        return None
