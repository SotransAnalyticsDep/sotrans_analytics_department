"""
Модуль отвечает за перехват и обработку ошибок, возникающую в процессе работы 
с файлами Excel.
"""

# ##################################################
# IMPORTS
# ##################################################
from typing import Callable, Any, Optional
from functools import wraps
from zipfile import BadZipFile

from pandas import DataFrame
from loguru import logger
import win32com.client

from source.utils.excel.vba import get_unmerge_all_cells


# ##################################################
# DECORATORS
# ##################################################
def handler_xl_shared_string_xml(func: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
    """
    Декоратор обработки ошибки при формировании DataFrame из XLSX-файлов.
    При возникновении ошибки "error_xl_shared_strings_xml", связанной с
    некорректной выгрузкой данных из 1С, в рабочую книгу Excel передаётся
    модуль с VBA-кодом, который рахъединяет все объединённые ячейки на всех
    листах, сохраняет файл и выполняет повторную попытку формирования DataFrame.

    :param func: Декорируемая функция, возвращающая pandas DataFrame
    :return: Callable[..., pd.DataFrame]: Обёрнутая функция с обработкой
    ошибки "error_xl_shared_strings_xml".
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> DataFrame:

        try:
            # Первая попытка чтения XLSX-файла и попытка формирования DataFrame;
            return func(*args, **kwargs)

        except (KeyError, BadZipFile) as e:
            logger.warning(
                f'Обнаружена ошибка "xl shared string xml" при чтении файла: {str(e)}.\n'
                + ' Запуск исправляющего VBA-скрипта.'
            )

            # Извлечение пути к файлу (учитывая, что первый аргумент может быть self);
            filepath: Optional[Any] = kwargs.get('filepath', None)

            # Если первый элемент — это метод класса, пропускаем его;
            if not filepath and len(args) > 1:
                filepath: Optional[Any] = args[1]

            # Если это функция, а не метод;
            elif not filepath and len(args) == 1:
                filepath: Optional[Any] = args[0]

            # Проверка, что filepath это строка;
            if not isinstance(filepath, str) and not filepath:
                logger.error(f'Некорректный путь к файлу;')
                raise ValueError(f'Некорректный путь к файлу;')

            # Запуск VBA-скрипта;
            try:
                # Загрузка рабочей книги Excel
                excel = win32com.client.Dispatch('Excel.Application')

                # Установка видимости отображения процесса
                excel.Visible = False

                # Открытие рабочей книги
                workbook = excel.Workbooks.Open(filepath)  # Workbook !!!

                # Добавление нового модуля VBA
                vba_module = workbook.VBProject.VBComponents.Add(1)

                # Передача VBA-кода для разъединения объединённых ячеек
                vba_module.CodeModule.AddFromString(get_unmerge_all_cells())

                # Выполнение VBA-макроса
                excel.Application.run('unmerge_all_cells')

                # Сохранение данных, выход из рабочей книги и закрытие приложения
                workbook.Save()
                workbook.Close()
                excel.Quit()

                logger.success('VBA-макрос успешно выполнен;')

                # Повторная попытка формирования DataFrame
                logger.info('Повторная попытка формирования DataFrame')
                return func(*args, **kwargs)

            except Exception as e:
                err_msg: str = f'Ошибка при выполнении VBA-макроса: {str(e)};'
                raise RuntimeError(err_msg) from e

    return wrapper
