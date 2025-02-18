"""
Модуль settings.py предназначен для загрузки и управления конфигурационными параметрами проекта.
Поддерживаются конфигурационные файлы в форматах JSON и YAML.

Пример использования:
    from source.config.settings import Settings

    # Укажите путь к конфигурационному файлу (например, config.yaml или config.json)
    settings = Settings("path/to/config.yaml")
    config = settings.load()

    # Получение параметра из конфигурации
    db_host = settings.get("database.host", default="localhost")
    print("Хост базы данных:", db_host)
"""

import os
import json

try:
    import yaml
except ImportError:
    yaml = None

from source.common.logger import Logger

# Инициализация логгера с указанием файла лога и уровня логирования
logger = Logger(log_file="app.log", level="DEBUG")
logger.info("Это информационное сообщение")



class Settings:
    """
    Класс для загрузки и управления настройками проекта.
    
    Атрибуты:
        config_filepath (str): Путь к конфигурационному файлу.
        config (dict): Загруженные параметры конфигурации.
    """
    
    def __init__(self, config_filepath: str) -> None:
        """
        Инициализирует экземпляр класса Settings.
        
        Args:
            config_filepath (str): Путь к конфигурационному файлу. Поддерживаемые форматы: JSON, YAML.
        
        Raises:
            FileNotFoundError: Если указанный файл конфигурации не найден.
        """
        if not os.path.exists(path=config_filepath):
            error_message: str = f"Конфигурационный файл '{config_filepath}' не найден."
            logger.error(error_message)
            raise FileNotFoundError(error_message)
        self.config_filepath: str = config_filepath
        self.config: dict = {}
    
    def load(self) -> dict:
        """
        Загружает конфигурационные параметры из файла.
        
        Определяет формат файла по расширению:
            - .yaml / .yml: используется модуль PyYAML (yaml.safe_load)
            - .json: используется стандартный модуль json (json.load)
        
        Returns:
            dict: Словарь с параметрами конфигурации.
        
        Raises:
            ValueError: Если формат файла не поддерживается или произошла ошибка при загрузке.
            ImportError: Если файл в фомате YAML, но модуль PyYAML не установлен.
        """
        file_ext: str = os.path.splitext(self.config_filepath)[1].lower()
        
        try:
            with open(file=self.config_filepath, mode='r', encoding='utf-8') as file:
                if file_ext in ('.yaml', '.yml'):
                    if yaml is None:
                        raise ImportError('Для поддержки YAML файлов установите пакет PyYAML (pip install pyyaml).')
                    self.config = yaml.safe_load(file)
                elif file_ext == '.json':
                    self.config = json.load(file)
                else:
                    raise ValueError('Неподдерживаемый формат файла. Поддерживаемые форматы: JSON, YAML')
        except Exception as e:
            raise ValueError(f'Ошибка при загрузке конфигурации: {e}')
        
        return self.config
    
    def get(self, key: str, default=None):
        """
        Возвращает значение параметра конфигурации по заданному ключу.
        
        Поддерживается вложенный доступ с использованием точечной нотации.
        Например, ключ "database.host" ищет в словаре конфигурации:
        config: dict = {"database": {"host": "localhost"}}
        
        Args:
            key (str): Ключ параметра.
            default: Значение по умолчанию, если ключ не найден.
        
        Returns:
            Значение параметра, если ключ найден, иначе default.
        """
        keys = key.split('.')
        value = self.config
        try:
            for k in keys:
                value = value[k]
        except (KeyError, ValueError):
            logger.warning(f'Ключ {key} не найден в конфигурационном файле.')
            return default
        return value