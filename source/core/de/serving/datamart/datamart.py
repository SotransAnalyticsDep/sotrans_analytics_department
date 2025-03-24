"""
Модуль отвечает за создание и взаимодействие с Витриной Данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
import sys
import datetime as dt
from typing import Tuple, Dict, Optional

import pandas as pd
from sqlalchemy import Engine
from loguru import logger

from modules.config import (
    DEFAULT_ENGINE,
    DEFAULT_PERIOD,
    DEFAULT_END_DATE,
    DEFAULT_AGG_CAT_COLS,
    DEFAULT_AGG_DGT_COLS,
    DEFAULT_AGG_DT_COLS,
    DEFAULT_AGG_FUNC
)
from modules.interface import (
    IStart,
    IIncome,
    IHaving,
    IExpend,
    IEnd,
    ITransfer,
    IArrival,
    IOrder,
    ISale,
    ILoss,
)


# ##################################################
# КЛАССЫ
# ##################################################
class Datamart:
    """
    Класс представляет интерфейсы для наполнения Витрины Данных.
    """


    def __init__(
            self,
            engine: Engine = DEFAULT_ENGINE,
            period: int = DEFAULT_PERIOD,
            end_date: dt.date = DEFAULT_END_DATE,
            agg_cat_cols: Tuple[str] = DEFAULT_AGG_CAT_COLS,
            agg_dgt_cols: Tuple[str] = DEFAULT_AGG_DGT_COLS,
            agg_dt_cols: Tuple[str] = DEFAULT_AGG_DT_COLS,
            agg_func: str = DEFAULT_AGG_FUNC
    ) -> None:


        # ##################################################
        # АРГУМЕНТЫ
        # ##################################################
        self.__engine: Engine = engine
        self.__period: int = period
        self.__end_date: dt.date = end_date,
        self.__start_date: dt.date = self.calc_start_date(period=period - 1)
        self.__agg_cat_cols: Tuple[str] = agg_cat_cols
        self.__agg_dgt_cols: Tuple[str] = agg_dgt_cols
        self.__agg_dt_cols: Tuple[str] = agg_dt_cols
        self.__agg_func: str = agg_func
        self.__df: pd.DataFrame = pd.DataFrame(columns=self.__agg_cat_cols)
        self.__interfaces: Dict[str, None] = {
            "IStart": None,     # Начальный остаток
            "IIncome": None,    # Приход
            "IHaving": None,    # Наличие
            "IExpend": None,    # Расход (продажи без цены реализации)
            "IEnd": None,       # Конечный остаток
            "ITransfer": None,  # Товары в пути
            "IArrival": None,   # Приёмка товаров
            "IOrder": None,     # Текущие размещённые заказы
            "ISale": None,      # Продажи (с ценой реализации)
            "ILoss": None,      # Упущенный спрос
        }


    # ##################################################
    # СВОЙСТВА АТРИБУТОВ
    # ##################################################
    @property
    def engine(self) -> Engine:
        """
        Геттер объекта подключения к базе данных.
        """
        return self.__engine
    
    @property
    def period(self) -> int:
        """
        Геттер значения анализируемого периода.
        """
        return self.__period
    
    @property
    def start_date(self) -> dt.date:
        """
        Геттер начальной даты анализируемого периода.
        """
        return self.__start_date
    
    @property
    def end_date(self) -> dt.date:
        """
        Геттер конечной даты анализируемого периода.
        """
        return self.__end_date
    
    @property
    def agg_cat_cols(self) -> Tuple[str]:
        """
        Геттер кортежа с наименованиями категориальных столбцов.
        """
        return self.__agg_cat_cols
    
    @property
    def agg_dgt_cols(self) -> Tuple[str]:
        """
        Геттер кортежа с наименованиями числовых столбцов.
        """
        return self.__agg_dgt_cols
    
    @property
    def agg_dt_cols(self) -> Tuple[str]:
        """
        Геттер кортежа с временными столбцами.
        """
        return self.__agg_dt_cols
    
    @property
    def agg_func(self) -> str:
        """
        Геттер значения функции агрегации.
        """
        return self.__agg_func
    
    @property
    def df(self) -> pd.DataFrame:
        """
        Геттер DataFrame с витриной данных.
        """
        return self.__df
    
    @df.setter
    def df(self, df: pd.DataFrame) -> None:
        """
        Сеттер нового значения self.__df.

        Args:
            df (pd.DataFrame): Новый DataFrame.
        """
        self.__df = df
        logger.success(f"Витрина данных обновлена: {self.__df.shape}")


    # ##################################################
    # СВОЙСТВА ЛЕНИВЫХ ИНТЕРФЕЙСОВ
    # ##################################################
    @property
    def IStart(self) -> IStart:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Начальный остаток".
        """
        return self.__get_interface(key="IStart", cls=IStart)
    
    @property
    def IIncome(self) -> IIncome:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Приход".
        """
        return self.__get_interface(key="IIncome", cls=IIncome)
    
    @property
    def IHaving(self) -> IHaving:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Наличие".
        """
        return self.__get_interface(key="IHaving", cls=IHaving)
    
    @property
    def IExpend(self) -> IExpend:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Расход".
        """
        return self.__get_interface(key="IExpend", cls=IExpend)
    
    @property
    def IEnd(self) -> IEnd:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Конечный остаток".
        """
        return self.__get_interface(key="IEnd", cls=IEnd)
    
    @property
    def ITransfer(self) -> ITransfer:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Товары в пути".
        """
        return self.__get_interface(key="ITransfer", cls=ITransfer)
    
    @property
    def IArrival(self) -> IArrival:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Приёмка товара".
        """
        return self.__get_interface(key="IArrival", cls=IArrival)
    
    @property
    def IOrder(self) -> IOrder:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Текущие расмещённые заказы".
        """
        return self.__get_interface(key="IOrder", cls=IOrder)
    
    @property
    def ISale(self) -> ISale:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Продажи" (с розничной ценой).
        """
        return self.__get_interface(key="ISale", cls=ISale)
    
    @property
    def ILoss(self) -> ILoss:
        """
        Создаёт и возвращает интерфейс для взаимодействия с данными "Упущенный спрос".
        """
        return self.__get_interface(key="ILoss", cls=ILoss)


    # ##################################################
    # ЗАЩИЩЁННЫЕ МЕТОДЫ
    # ##################################################
    def __get_interface(self, key: str, cls) -> object:
        """
        Общий метод ленивого создание интерфейса.
        
        Args:
            key(str): Наименования интерфейса из словаря self.__interfaces.
            cls: Класс интерфейса для создания экземпляра.
        
        Returns:
            object: Экземпляр указанного класса интерфейса.
        """
        if self.__interfaces[key] is None:
            logger.debug(f"Создание интерфейса: {key}")
            self.__interfaces[key] = cls(self)
            logger.success(f"Интерфейс {key} успешно создан")
        
        return self.__interfaces[key]


    # ##################################################
    # API
    # ##################################################
    def calc_start_date(
            self,
            period: Optional[int] = None,
            end_date: Optional[dt.date] = None
    ) -> dt.date:
        """
        Рассчитывает начальную дату анализируемого диапазона на основе заданного периода.

        Args:
            period (int, необязательный): Количество месяцев, которое нужно вычесть из
                конечной даты. Если None, используется период по умолчанию (__period).
                По умолчанию None.

        Return:
            dt.date: Рассчитанная начальная дата, установленная на первый день получившегося месяца.

        Notes:
            Расчет преобразует конечную дату (self.__end_date) в общее количество месяцев,
            вычитает указанный период, а затем преобразует результат обратно в год и месяц.
            Если получившийся месяц равен 0, он корректируется до декабря предыдущего года.
        """
        
        # Если period не установлен, значение берётся из экземпляра класса Datamart
        if period is None:
            period: int = self.__period
            logger.debug(f"Период не задан, используется значение из Datamart: {self.__period}")
        
        # Если end_date не установлена, значение берётся из экземпляра класса Datamart
        if end_date is None:
            end_date: dt.date = self.__end_date
            logger.debug(f"Конечная дата анализируемого периода не задана, используется значение из Datamart: {self.__end_date}")
        
        # Расчёт общего количествла месяцев и преобразование в год и месяц
        total_month: int = end_date.year * 12 + end_date.month - period
        start_date_year: int = total_month // 12
        start_date_month: int = total_month % 12
        
        # Корректировка, если месяц равен 0
        if start_date_month == 0:
            start_date_month = 12
            start_date_year -= 1
            logger.debug(f"Месяц скорректирован до декабря предыдущего года: {start_date_year}.{start_date_month}")
        
        # Формирование начальной даты анализируемого периода
        start_date: dt.date = dt.date(
            year=start_date_year,
            month=start_date_month,
            day=1
        )
        logger.success(f"Начальная дата анализируемого периода успешно рассчитана: {start_date}")
        
        return start_date
