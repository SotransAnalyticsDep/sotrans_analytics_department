"""
Модуль отвечает за обработку ошибки error_xl_shared_strings_xml, возникающую при формировании DataFrame из xlsx-файла, сформированного в 1С.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
import sys
import win32com.client
from functools import wraps
from typing import Collable, Any
from zipfile import BadZipFile

import pandas as pd
from loguru import logger

sys.path.append(r'C:\Users\user\Desktop\github\sotrans_analytics_department')
from source.utils.excel.vba import VBA_UNMERGE_ALL_CELLS


# ##################################################
# ФУНКЦИИ-ДЕКОРАТОРЫ
# ##################################################
def error_xl_shared_strings_xml(func: Collable[..., pd.DataFrame]) -> Collable[..., pd.DataFrame]:
    """
    Декоратор обработки ошибки при формировании DataFrame из XLSX-файлов.
    При возникновении ошибки "error_xl_shared_strings_xml", связанной с некорректной выгрузкой данных из 1С, в рабочую книгу Excel передаётся модуль с VBA-кодом, который рахъединяет все объединённые ячейки на всех листах, сохраняет файл и выполняет повторную попытку формирования DataFrame.
    
    Args:
        func(Callable): Декорируемая функция, возвращающая pandas DataFrame
    
    Returns:
        Callable[..., pd.DataFrame]: Обёрнутая функция с обработкой ошибки "error_xl_shared_strings_xml".
    """
    
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
        try:
            # Первичня попытка формирования DataFrame
            return func(*args, **kwargs)
        
        except (KeyError, BadZipFile) as err:
            logger.warning(f'Обнаружена ошибка "error_xl_shared_strings_xml" при чтении файла: {str(err)}. Запуск исправляющего VBA-макроса.')
            
            # Извлечение пути к файлу (учитывая, что первый аргумент может быть self)
            filepath: str = kwargs.get('filepath', None)
            # Если это метод класса, пропускаем self (args[0])
            if not filepath and len(args) > 1:
                filepath: str = args[1]
            # Если это функция, а не метод
            elif not filepath and len(args) == 1:
                filepath: str = args[0]
            
            if not isinstance(filepath, str) and not filepath:
                err_msg: str = f'Путь к файлу "filepath" не был корректно передан: {filepath}'
                logger.error(err_msg)
                raise ValueError(err_msg)

            try:
                # Загрузка рабочей книги Excel
                excel = win32com.client.Dispatch('Excel.Application')
                # Установка видимости отображения процесса
                excel.Visible = False
                # Открытие рабочей книги
                workbook = excel.Workbook.Open(filepath)
                # Добавление нового модуля VBA
                vba_module = workbook.VBProject.VBComponents.Add(1)
                # Передача VBA-кода для разъединения объединённых ячеек
                vba_module.CodeModule.AddFromString(VBA_UNMERGE_ALL_CELLS)
                # Выполнение VBA-макроса
                excel.Application.run('unmerge_all_cells')
                # Сохранение данных, выход из рабочей книги и закрытие приложения
                workbook.Save()
                workbook.Close()
                excel.Quit()
                
                logger.success('VBA-макрос успешно выполнен')
                
                # Повторная попытка формирования DataFrame
                logger.info('Повторная попытка формирования DataFrame')
                return func(*args, **kwargs)
                
            except Exception as vba_err:
                vba_err_msg: str = f'Ошибка при выполнении VBA-макроса: {str(vba_err_msg)}'
                raise RuntimeError(vba_err_msg)
        
        return wrapper
