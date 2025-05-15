import pandas as pd
import sqlalchemy as sa
from loguru import logger

class CheckExistsDataInTable:
    
    def batch_movement_by_date(self, prefix: str, doc_type: str, report_date: str) -> bool:
        """
        Функция проверяет наличие данных в базе за отчётную дату.
        
        Args:
            report_date (str| dt.datetime): Отчётная дата.
        """
        
        # Создание движка подключения
        engine = sa.create_engine("postgresql+psycopg2://postgres:30691@localhost:5432/one_c")
        
        # SQL-запрос на проверку наличия данных за отчётную дату.
        sql_query: str = f"""
        SELECT
            CAST(CASE WHEN EXISTS (
                SELECT 1 FROM report.bm_{prefix}_{doc_type}
                    WHERE date = '{report_date}')
                THEN 1 ELSE 0
                END AS bit
            ) AS IsDataExist
        ;
        """
        
        # Вызов SQL-запроса.
        with engine.begin() as connection:
            try:
                result: pd.DataFrame = pd.read_sql_query(
                    sql=sql_query,
                    con=connection,
                )
            except sa.exc.ProgrammingError:
                logger.warning('Таблица "bm_st_init" отсутствует в базе')
                return False
        
        return result.loc[0, 'isdataexist'] == '1'
