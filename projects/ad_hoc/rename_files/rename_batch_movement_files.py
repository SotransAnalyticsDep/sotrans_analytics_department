"""
Модуль отвечает за переименование файлов документов движения с формата %d.%m.%Y в формат %Y-%m-%d.
"""
# ##################################################
# IMPORTS
# ##################################################
import os
import datetime as dt

from loguru import logger

# ##################################################
# CONSTANTS
# ##################################################
SRC_DIR: str = r'C:\Users\user\YandexDisk\batch_movement\batch_movement'

# ##################################################
# FUNCTIONS
# ##################################################
def check_filename(filename: str) -> bool:
    """
    Проверяет соответствие имени файла формату %d.%m.%Y.

    Args:
        filename (str): Название файла.
    
    Returns:
        bool: True, если имя файла соответствует формату %d.%m.%Y, иначе False.
    """
    try:
        dt.datetime.strptime(filename, '%d.%m.%Y.xlsx')
        logger.warning(f'Требуется переименование файла: {filename}')
        return True
    except ValueError:
        logger.debug(f'Файл {filename} не требует переименования')
        return False

def main() -> None:
    """
    Переименовывает файлы документов движения в формате %d.%m.%Y.xlsx в формат %Y.%m.%d.xlsx
    """
    
    for filename in os.listdir(SRC_DIR):
        if filename.endswith('.xlsx') and not filename.startswith('~'):
            if check_filename(filename=filename):
                day, month, year, _ = filename.split('.')
                new_name: str = f'{year}.{month}.{day}.xlsx'
                logger.debug(f'Новой имя файла: {new_name}')
                
                try:
                    os.rename(
                        os.path.join(SRC_DIR, filename),
                        os.path.join(SRC_DIR, new_name)
                    )
                    logger.success(f'Файл {filename} переименован в {new_name}')
                
                except Exception as err:
                    logger.error(f'Ошибка при переименовании файла {filename}: {err}')

# ##################################################
# ENTRY POINT
# ##################################################
if __name__ == '__main__':
    main()
    logger.success('Готово!')