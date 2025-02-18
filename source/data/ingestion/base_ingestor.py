"""
Модуль base_ingestor.py содержит абстрактный базовый класс BaseIngestor предназначен 
для определения интерфейс классов загрузчиков:
    - CSVIngestor
    - JSONIngestor
    - XMLIngestor
    - SQLIngestor
Пример использования:
    class CSVIngestor(BaseIngestor):
        def ingest(self,**kwargs) -> Any:
        
    сlass JSONIngestor(BaseIngestor):
        def ingest(self,**kwargs) -> Any:
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseIngestor(ABC):
    
    @abstractmethod
    def ingest(self, path: str) -> Any:
        """
        Абстрактный метод для загрузки данных из источника.

        :param source: Путь к источнику данных (например, файл или URL).
        :return: Загруженные данные в формате, зависящем от реализации.
        """
        NotImplementedError("Необходимо реализовать метод ingest()")
        
