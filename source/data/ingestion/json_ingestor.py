"""
Модуль json_ingestor.py предназначен для настройки конфигурации считывания и загрузки json файла .

Пример использования:
    from source.data.json_ingestor import JSONIngestor

    # Укажите путь к считываему файлу и параметры его настройки:
    ingestor = JSONingestor()
    data = ingestor.ingest(filepath_or_buffer ="path/to/file.csv", delimiter=",", encoding="utf-8")

    # Получение параметра из конфигурации
    db_host = settings.get("database.host", default="localhost")
    print("Хост базы данных:", db_host)
"""



from typing import Optional
import warnings

import pandas as pd

from .base_ingestor import BaseIngestor
from source.common.logger import Logger



class JSONIngestor(BaseIngestor):
    """Класс

    Args:
        BaseIngestor - абстрактный базовый класс для загрузки данных из различных источников



    """

    def ingest(self,**kwargs) -> Optional[pd.DataFrame]:

        """Загружает данные из JSON файла и возвращает их в виде датафрейма.

        Args:
            **kwargs: Параметры загрузки JSON файла. Возможные параметры:
                path_or_buf (str):
                    Путь к JSON файлу.
                orient (str, optional):
                     Определяет формат JSON (определяется автоматически по умолчанию).
                typ (str, optional):
                    Тип возвращаемого объекта: 'frame' (DataFrame, по умолчанию) или 'series' (Series).
                convert_dates (list, optional):
                    Указывает, какие столбцы преобразовать в даты.
                keep_default_dates (bool, optional):
                    Если True, автоматически преобразует столбцы, похожие на даты, в тип datetime.
                encoding (str, optional):
                    Указывает кодировку файла (например, 'utf-8').
                numpy (bool, optional):
                    Если True, возвращает данные в виде массивов NumPy вместо объектов Pandas.

        Returns:
            pd.DataFrame:
                Датафрейм с данными из JSON файла.

        Raises:
            FileNotFoundError:
                Если файл не найден.
            pd.errors.ParserError:
                Если файл не может быть прочитан.
            RuntimeError:
                Если возникла неизвестная ошибка.

        Examples:
            Пример использования:
            >>> df = load_json_data(path_or_buf='data.json', orient='records', typ='frame')
        """
        logger = Logger()
        logger.info("Загрузка данных из JSON файла")
        logger.debug(f"Параметры загрузки JSON файла : {kwargs}")
        try:
            df:pd.DataFrame = (
                pd.read_json(**kwargs)
                               )
            logger.success(f"Данные успешно загружены,размер датфрейма{df.shape}")
            return df
        except FileNotFoundError as e:
            error_message = f"Файл по ссылке {kwargs} не найден"
            logger.error(error_message)
            raise FileNotFoundError(error_message) from e
        except Exception as e:
            error_message = f"Неизвестная ошибка{e}"
            logger.error(error_message)
            raise RuntimeError(error_message) from e