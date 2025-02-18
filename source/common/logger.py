"""
Модуль logger.py предоставляет централизованное логирование для проекта с использованием библиотеки loguru.
Он реализует паттерн Singleton для обеспечения единственного экземпляра логгера в приложении, что позволяет
консистентно настраивать логирование во всех модулях.

Основные возможности:
- Логирование сообщений в консоль и в файл.
- Автоматическая ротация файлов (при достижении заданного размера).
- Сжатие старых логов и их удержание в течение указанного времени.
- Поддержка стандартных уровней логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL.

Пример использования:
    from source.common.logger import Logger

    # Инициализация логгера с указанием файла лога и уровня логирования
    logger = Logger(log_file="app.log", level="DEBUG")
    logger.info("Это информационное сообщение")
"""

from loguru import logger as loguru_logger


class Logger:
    """
    Класс Logger является оберткой над loguru, реализует паттерн Singleton для обеспечения единственного экземпляра логгера.

    Attributes:
        log_file (str): Путь к файлу для сохранения логов.
        level (str): Уровень логирования.
    """
    _instance = None

    def __new__(cls, log_file: str = "app.log", level: str = "DEBUG"):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, log_file: str = "app.log", level: str = "DEBUG"):
        if self._initialized:
            return  # Избегаем повторной инициализации при использовании Singleton
        self._initialized = True

        self.log_file = log_file
        self.level = level

        # Удаляем все предустановленные обработчики loguru
        loguru_logger.remove()

        # Добавляем обработчик для вывода логов в консоль
        loguru_logger.add(
            lambda msg: print(msg, end=""),
            level=self.level,
            enqueue=True,
            #format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )

        # Добавляем обработчик для записи логов в файл
        loguru_logger.add(
            self.log_file,
            level=self.level,
            rotation="10 MB",      # Файл будет ротироваться при достижении 10 МБ
            retention="10 days",     # Сохранять ротации логов в течение 10 дней
            compression="zip",       # Сжимать старые файлы логов в формате zip
            enqueue=True,
            #format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} - {message}"
        )

    def debug(self, message: str, *args, **kwargs):
        """
        Логирует отладочное сообщение.
        Args:
            message (str): Сообщение для логирования.
            *args, **kwargs: Дополнительные аргументы, передаваемые loguru.
        """
        loguru_logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        """
        Логирует информационное сообщение.
        Args:
            message (str): Сообщение для логирования.
            *args, **kwargs: Дополнительные аргументы, передаваемые loguru.
        """
        loguru_logger.info(message, *args, **kwargs)
    
    def success(self, message: str, *args, **kwargs):
        """
        Логирует успешное сообщение.
        Args:
            message (str): Сообщение для логирования.
            *args, **kwargs: Дополнительные аргументы, передаваемые loguru.
        """
        loguru_logger.success(message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        """
        Логирует предупреждение.
        Args:
            message (str): Сообщение для логирования.
            *args, **kwargs: Дополнительные аргументы, передаваемые loguru.
        """
        loguru_logger.warning(message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        """
        Логирует сообщение об ошибке.
        Args:
            message (str): Сообщение для логирования.
            *args, **kwargs: Дополнительные аргументы, передаваемые loguru.
        """
        loguru_logger.error(message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        """
        Логирует критическое сообщение.
        Args:
            message (str): Сообщение для логирования.
            *args, **kwargs: Дополнительные аргументы, передаваемые loguru.
        """
        loguru_logger.critical(message, *args, **kwargs)


# Пример использования данного модуля при его запуске напрямую.
if __name__ == "__main__":
    logger = Logger(log_file="app.log", level="DEBUG")
    logger.debug("Это отладочное сообщение")
    logger.info("Это информационное сообщение")
    logger.warning("Это предупреждение")
    logger.error("Это сообщение об ошибке")
    logger.critical("Это критическое сообщение")
