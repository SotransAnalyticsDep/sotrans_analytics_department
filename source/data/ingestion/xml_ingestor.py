"""
Модуль xml_ingestor.py предназначен для настройки конфигурации считывания и загрузки EXCEL файла .

Пример использования:
    from source.data.csv_ingestor import XMLIngestor

    # Укажите путь к считываему файлу и параметры его настройки:
        ingestor = XMLingestor()
        data = ingestor.ingest(io ="path/to/file.csv", sheet_name="Sheet1", encoding ="utf-8")
        display(data)
"""

from typing import Optional
import warnings

from .base_ingestor import BaseIngestor
import pandas as pd

from .base_ingestor import BaseIngestor
from source.common.logger import Logger






class XMLIngestor(BaseIngestor):
    """Класс

    Args:
        BaseIngestor (_type_): _description_
    """

    def ingest(self,**kwargs) -> Optional[pd.DataFrame]:
        # sourcery skip: raise-from-previous-error

        """
            Загрузка данных из EXCEL файла и возвращение их в виде датафрейма.

            **kwargs: Параметры загрузки EXCEL файла

            Примеры(**kwargs):
            ----------
            io : str или file-like объект
                Путь к  файлу или объект с данными.
            sheet_name :Тип: str, int, list, or None
                Параметр выбора листа. По умолчанию None.
            engine : str, optional
                Движок для загрузки EXCEL файла(чаще используется openpyxl). По умолчанию None.
            encoding : str, optional
                Кодировка файла. По умолчанию 'utf-8'.
            header : int или list of int, optional
                Номер строки, которая будет использоваться как заголовок. 
                Если 'infer', заголовок определяется автоматически. По умолчанию 'infer'.
            skiprows : int или list of int, optional
                Количество строк, которые будут пропущены при чтении. По умолчанию None.
            names : list-like, optional
                Список имен для столбцов.
            index_col : int, str, list of int,
               Номер столбца или список столбцов, которые будут использоваться как индекс.
            usecols:int, str, list-like, or callable, optional
                Указывает, какие столбцы загружать

            Возвращает
            -------
            pd.DataFrame
                Датафрейм с данными из EXCEL файла.

            Примеры
            --------
            >>> df = read_csv("data.csv", sep=";", encoding="latin1")
            >>> df.head()
                """
        logger = Logger()
        logger.info("Загрузка данных из EXCEL файла")
        logger.debug(f"Параметры загрузки EXCEL файла : {kwargs}")
        try:
                    # Перехват предупреждений и преобразование их в исключения
            with warnings.catch_warnings():
                warnings.filterwarnings('error', category=pd.errors.ParserWarning)
                df: pd.DataFrame =(
                    pd.read_excel(**kwargs)
                )
            logger.success(f"Данные успешно загружены,размер датфрейма{df.shape}")
            return df
        except FileNotFoundError as e:
            error_message = f"Файл по ссылке {kwargs} не найден"
            logger.error(error_message)
            raise FileNotFoundError(error_message) from e
        except Exception as e:
            error_message = f"Неизвестная ошибка: {e}"
            logger.error(error_message)
            raise RuntimeError(error_message) from e
        