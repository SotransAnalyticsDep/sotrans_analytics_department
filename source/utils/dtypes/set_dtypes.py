"""
Модуль отвечает за автоматическое определение и установку корректных типов данных.
"""

# ##################################################
# IMPORTS
# ##################################################
import pandas as pd
from loguru import logger


# ##################################################
# FUNCTIONS
# ##################################################
def set_category_dtype(
        dataframe: pd.DataFrame,
        columns: tuple[str, ...],
) -> pd.DataFrame:
    """
    Преобразование типов данных в категориальные для указанных столбцов в DataFrame.

    Args:
        dataframe: Исходный DataFrame.
        columns: Кортеж с наименованиями столбцов, которые необходимо преобразовать к категориальному типу данных.
    
    Returns:
        pd.DataFrame: DataFrame с преобразованными типами данных.
    """
    
    
    # Рассчитать значение трети DataFrame;
    thrd_part: int = dataframe.shape[0] // 3
    logger.trace(f'Количество строк в DataFrame: {dataframe.shape[0]};')
    logger.trace(f'Размер трети DataFrame: {thrd_part}')
    
    # Проверка наличия переданных наименований столбцов в DataFrame;
    for column in columns:
        if column not in dataframe.columns:
            logger.error(f'Неверно указано название столбца \"{column}\";')
            raise ValueError(f'Неверно указано название столбца \"{column}\";')
        
    # Определение необходимости и преобразование типов данных к категориальному;
    for column in columns:
        column_type = dataframe[column].dtype
        logger.trace(f'Тип данных для столбца \"{column}\": {column_type}.')
        
        # Если тип данных для столбца уже является категориальным, то преобразование не требуется.
        if column_type == 'category':
            logger.info(f'Тип данных для столбца \"{column}\" уже является категориальным;')
            continue
        
        # Определение количества уникальных значений в выбранном столбце;
        unq_val_cnt: int = dataframe[column].nunique()
        logger.trace(f'Количество уникальных значений: {unq_val_cnt}.')
        
        # Если количество уникальных значений менее трети строк датафрейма, то преобразовать столбец к категориальному типу данных.
        if unq_val_cnt <= thrd_part:
            logger.debug('Количество уникальных значений менее трети строк датафрейма;')
            logger.info(f'Преобразование столбца \"{column}\" к категориальному типу данных;')
            
            # Преобразование типа данных к категориальному;
            try:
                dataframe[column] = dataframe[column].astype(dtype='category')
                logger.success(f'Столбец \"{column}\" преобразован к категориальному типу данных;')
            except Exception as e:
                logger.error(f'Непредвиденная ошибка: {e};')
        
        # Если количество уникальных значений больше трети строк датафрейма, то преобразование типа данных не требуется.
        else:
            logger.debug('Количество уникальных значений больше трети строк датафрейма;')
            logger.info(f'Преобразование типов данных для столбца \"{column}\" не требуется;')
    
    return dataframe
