"""
Модуль сsv_ingestor.py предназначен для настройки конфигурации считывания и загрузки csv файла .

Пример использования:
    from source.data.csv_ingestor import CSVIngestor

    # Укажите путь к считываему файлу и параметры его настройки:
    ingestor = CSVingestor()
    data = ingestor.ingest(filepath_or_buffer ="path/to/file.csv", sep=",", encoding ="utf-8")
    display(data)
"""

from typing import Optional
import warnings

import pandas as pd 

from .base_ingestor import BaseIngestor
from source.common.logger import Logger


class CSVIngestor(BaseIngestor):
    """
    Класс для загрузки данных из CSV файла.
    """
    def ingest(self,**kwargs) -> Optional[pd.DataFrame]:
        """
        Загрузка данных из CSV файла и возвращение их в виде датафрейма.

        **kwargs: Параметры загрузки JSON файла

        Примеры(**kwargs):
        ----------
        filepath_or_buffer : str или file-like объект
            Путь к CSV файлу или объект с данными.
        sep : str, optional
            Разделитель полей в CSV файле. По умолчанию ','.
        encoding : str, optional
            Кодировка файла. По умолчанию 'utf-8'.
        header : int или list of int, optional
            Номер строки, которая будет использоваться как заголовок. 
            Если 'infer', заголовок определяется автоматически. По умолчанию 'infer'.
        skiprows : int или list of int, optional
            Количество строк, которые будут пропущены при чтении. По умолчанию None.
        dtype : dict, optional
            Словарь для указания типов данных колонок. Например: {"column_name": "int64"}. 
            По умолчанию None.
        engine : str, optional
            Движок для чтения CSV. Возможные значения: 'c', 'python'. По умолчанию None (автовыбор).

        Возвращает
        -------
        pd.DataFrame
            Датафрейм с данными из CSV файла.

        Примеры
        --------
        >>> df = read_csv("data.csv", sep=";", encoding="latin1")
        >>> df.head()
        """
        logger = Logger()
        logger.info("Загрузка данных из CSV файла")
        logger.debug(f"Параметры загрузки CSV файла : {kwargs}")
        try:
            # Перехват предупреждений и преобразование их в исключения
            with warnings.catch_warnings():
                warnings.filterwarnings('error', category=pd.errors.ParserWarning)
                df: pd.DataFrame = pd.read_csv(**kwargs)
            logger.success(f"Данные успешно загружены, размер датафрейма: {df.shape}")
            return df

        except FileNotFoundError as e:
            error_message = f"Файл по ссылке {kwargs.get('filepath_or_buffer')} не найден"
            logger.error(error_message)
            raise FileNotFoundError(error_message) from e
        except pd.errors.ParserWarning as e:
            error_message = f"Ошибка при загрузке данных(проверьте sep:({kwargs.get('sep')}) или 'engine' читаемого файла) : {e}"
            logger.warning(error_message)
            # Возвращаем пустой DataFrame или обрабатываем предупреждение
            return pd.DataFrame()
        except Exception as e:
            error_message = f"Неизвестная ошибка: {e}"
            logger.error(error_message)
            raise RuntimeError(error_message) from e
    

  
                 




