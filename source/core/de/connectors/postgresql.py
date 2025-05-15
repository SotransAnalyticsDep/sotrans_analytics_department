"""
Модуль отвечает за создание подключения к базе данных PostgreSQL.
"""

# ##################################################
# IMPORTS
# ##################################################
from typing import Any

from sqlalchemy import Engine, create_engine
from loguru import logger
from yaml import safe_load


# ##################################################
# CLASSES
# ##################################################
class PGConnector:
    """
    Класс отвечает за создание подключения к базе данных PostgreSQL.
    """

    def __init__(
            self,
            config_path: str = r'C:\Users\user\Desktop\github\sotrans_analytics_department\source\_configs\database.yaml',
            db_name: str = 'one_c',
            is_echo: bool = False
    ) -> None:
        """
        Инициализация класса.

        Args:
            config_path (str, optional): Путь к файлу конфигурации. По умолчанию: r'C:\\Users\\user\\Desktop\\github\\new\\source\\config\\database.yaml'.
            db_name (str, optional): Название базы данных. По умолчанию: 'one_c'.
            is_echo (bool, optional): Выводить ли логи запросов в консоль. По умолчанию: False.
        """

        self.__src_path: str = config_path
        self.__database_name: str = db_name
        self.__is_echo: bool = is_echo
        self._engine: Engine = self.__create_engine()
    
    
    # ##################################################
    # PRIVATE METHODS
    # ##################################################
    def __create_connection_string(self) -> str:
        """
        Метод формирует строку подключения к базе данных.

        Raises:
            ValueError: Непредвиденная ошибка при создании строки подключения к базе данных.

        Returns:
            str: Строка подключения к базе данных.
        """

        try:
            logger.info('Начало формирования строки подключения к базе данных.')
            with open(file=self.__src_path, mode='r', encoding='utf-8') as file:
                config: Any = safe_load(stream=file)['postgresql']
                connection_string: str = (
                    f'{config['database']}+'
                    + f'{config['driver']}://'
                    + f'{config['login']}:'
                    + f'{config['password']}@'
                    + f'{config['host']}:'
                    + f'{config['port']}/'
                    + f'{self.__database_name}'
                )
                logger.success('Строка подключения к базе данных успешно создана.')
                return connection_string

        except Exception as e:
            err_msg: str = 'Непредвиденная ошибка при создании строки подключения к базе данных'
            logger.error(f'{err_msg}: {e}')
            raise ValueError(err_msg)


    def __create_engine(self) -> Engine:
        """
        Метод создаёт подключение к базе данных.

        Raises:
            ValueError: Непредвиденная ошибка при создании подключения к базе данных.

        Returns:
            Engine: Подключение к базе данных.
        """

        try:
            logger.info('Начало создания подключения к базе данных.')
            connection_string: str = self.__create_connection_string()
            engine: Engine = create_engine(url=connection_string, echo=self.__is_echo)
            logger.success('Подключение к базе данных успешно создано.')
            return engine

        except Exception as e:
            err_msg: str = 'Непредвиденная ошибка при создании подключения к базе данных'
            logger.error(f'{err_msg}: {e}')
            raise ValueError(err_msg)


    # ##################################################
    # PROPERTIES
    # ##################################################
    @property
    def engine(self) -> Engine:
        """
        Геттер подключения к базе данных.

        Returns:
            Engine: Подключение к базе данных.
        """
        return self._engine
