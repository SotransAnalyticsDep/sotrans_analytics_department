# 1. Imports
# Встроенные библиотеки
import os
import re
import math
import datetime
import win32com.client
from functools import wraps
from typing import Callable, Any
from zipfile import BadZipFile

import pandas as pd
from loguru import logger

# Сторонние библиотеки
import tqdm

# 2. Settings
## 2.1. Pypl
## 2.2. Constants
LIST_WITH_CITYS: list[str] = [
    "Екатеринбург",
    "Красноярск",
    "Мурманск",
    "Нижний Новгород",
    "Новосибирск",
    "Ростов",
    "СПб",
    "БУ",
]

# Заглушка для файлов без фото
COMMON_PIC_URL: str = "https://i.postimg.cc/2S2pVV8v/spb1.jpg"

# 2.3. Variables
# Время начала выполнения скрипта
start_time: datetime.datetime = datetime.datetime.now()

VBA_UNMERGE_ALL_CELLS: str = """
Sub unmerge_all_cells()
    Dim ws As Worksheet
    For Each ws In ThisWorkbook.Worksheets
        ws.Cells.UnMerge
    Next ws
End Sub
"""


def error_xl_shared_strings_xml(
    func: Callable[..., pd.DataFrame],
) -> Callable[..., pd.DataFrame]:
    """Декоратор для обработки ошибок при чтении Excel-файлов.

    При возникновении ошибок, связанных с объединенными ячейками или структурой файла,
    запускает VBA-макрос для разъединения ячеек и повторяет попытку вызова функции.

    Args:
        func: Декорируемая функция, возвращающая pandas DataFrame.

    Returns:
        Callable[..., pd.DataFrame]: Обернутая функция с обработкой ошибок.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
        try:
            # Первичная попытка выполнения функции
            return func(*args, **kwargs)
        except (KeyError, BadZipFile) as error:
            logger.warning(
                f"Обнаружена ошибка при чтении файла: {str(error)}. Запуск исправляющего макроса Excel"
            )

            # Извлекаем путь к файлу (учитываем, что первый аргумент может быть self)
            file_path: str = kwargs.get("filepath", None)
            if (
                not file_path and len(args) > 1
            ):  # Если это метод класса, пропускаем self (args[0])
                file_path = args[1]
            elif not file_path and len(args) == 1:  # Если это функция, а не метод
                file_path = args[0]

            if not isinstance(file_path, str) or not file_path:
                error_message: str = (
                    f"Путь к файлу (file_path) не был корректно передан: {file_path}"
                )
                logger.error(error_message)
                raise ValueError(error_message)

            try:
                # Запуск Excel в невидимом режиме
                excel = win32com.client.Dispatch("Excel.Application")
                excel.Visible = False
                workbook = excel.Workbooks.Open(file_path)

                # Добавление нового модуля VBA
                vba_module = workbook.VBProject.VBComponents.Add(1)

                # VBA-макрос для отмены объединённых ячеек
                macro_code: str = VBA_UNMERGE_ALL_CELLS
                vba_module.CodeModule.AddFromString(macro_code)

                # Выполнение макроса
                excel.Application.Run("unmerge_all_cells")

                # Сохранение и закрытие файла
                workbook.Save()
                workbook.Close()
                excel.Quit()

                logger.success(
                    "Макрос успешно выполнен, повторная попытка формирования датафрейма"
                )

                # Повторный вызов исходной функции
                return func(*args, **kwargs)

            except Exception as macro_error:
                error_message: str = f"Ошибка при выполнении макроса: {macro_error}"
                logger.error(error_message)
                raise RuntimeError(error_message)

    return wrapper


# 2.4. Functions
def catalog_number_false_symbols(catalog_number: str, clearing_type: str) -> str | None:
    """Функция принимает на вход два параметра: номер, который требуется очистить и тип очистки.
    - Для "origin" предусмотрена обычная очистка от символов пунктуации.
    - Для "sotrans" предусмотрена предварительная сепарация кода.


    :param clearing_type:
    :return:
    """

    # sourcery skip: use-fstring-for-concatenation

    from string import punctuation

    temp_text: str = catalog_number

    match clearing_type:
        case "origin":
            for false_symbol in punctuation + " ":
                temp_text = str(temp_text).lower().replace(false_symbol, "")

            return temp_text

        case "sotrans":
            for false_symbol in punctuation + " ":
                temp_text = (
                    str(temp_text).lower().split("_", -1)[0].replace(false_symbol, "")
                )

            return temp_text


@error_xl_shared_strings_xml
def create_dataframe(filepath: str) -> pd.DataFrame:
    df = pd.read_excel(
        io=filepath,
        engine="openpyxl",
        header=None,
        converters={
            0: str,
            1: str,
            2: str,
            3: str,
            4: str,
            5: str,
            6: str,
            7: str,
            8: str,
            9: str,
        },
    )

    return df


# 2.5. Path & Links
# Путь к директории с прайс-листами Сотранса
PATH_TO_FOLDER_WITH_SOTRANS_PRICELISTS: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files\sotrans"

# Путь к директории со справочниками
PATH_TO_FOLDERS_WITH_LOOKUPS: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files\lookups"

# Путь к директории с итоговыми файлами
PATH_TO_FOLDER_WITH_RESULT_FILES: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files\result"

# 3. Loads
## 3.1. Lookups
df_avito_settings: pd.DataFrame = pd.read_excel(
    io=rf"{PATH_TO_FOLDERS_WITH_LOOKUPS}\avito_settings.xlsx", engine="openpyxl"
)
df_avito_settings: pd.DataFrame = df_avito_settings
# Ссылки на фотографии
df_urls: pd.DataFrame = pd.read_json(
    path_or_buf=rf"{PATH_TO_FOLDERS_WITH_LOOKUPS}\photo_urls.json"
)

# 4. Main Program
for city in LIST_WITH_CITYS:
    # Пустой список для загруженных датафреймов (для конкатенации)
    list_with_dataframes: list[pd.DataFrame] = []

    for sotrans_filename in os.listdir(path=PATH_TO_FOLDER_WITH_SOTRANS_PRICELISTS):
        if re.match(pattern=city.lower(), string=sotrans_filename.lower()):
            list_with_dataframes.append(
                create_dataframe(
                    filepath=os.path.join(
                        PATH_TO_FOLDER_WITH_SOTRANS_PRICELISTS, sotrans_filename
                    ),
                )
            )
        else:
            continue

    if list_with_dataframes:
        # Пустой датафрейм для итоговой таблицы
        df_result: pd.DataFrame = pd.DataFrame()

        # Формирование датафрейма из прайс-листов "Европа" и "Америка"
        temp_sotrans_df: pd.DataFrame = pd.concat(list_with_dataframes)

        # Переименование столбцов
        temp_sotrans_df = temp_sotrans_df.rename(
            columns={
                0: "old_index",
                1: "product_id_1c",
                2: "product_oem_number",
                3: "creator_number",
                4: "product_catalog_number",
                5: "product_name",
                6: "cross_numbers",
                7: "brand",
                8: "price",
                9: "having",
            }
        )

        # Удаление столбца с индексами 1С
        temp_sotrans_df = temp_sotrans_df.drop(columns="old_index")

        # Удаление строк, полностью состоящих из NaN
        temp_sotrans_df = temp_sotrans_df.dropna(how="all")

        # Удалить строки с заголовками
        temp_sotrans_df = temp_sotrans_df.drop(
            index=temp_sotrans_df[temp_sotrans_df["product_id_1c"] == "Код 1С"].index
        )

        # Сбросить индексы
        temp_sotrans_df = temp_sotrans_df.reset_index(drop=True)

        # Добавить столбец с городом
        temp_sotrans_df["KEY_city"] = city

        # Добавление данных из файла настроек
        df_union: pd.DataFrame = temp_sotrans_df.merge(
            right=df_avito_settings, how="inner", on="KEY_city"
        )

        ######################

        if city == "Санкт-Петербург":
            df_union["Description"] = [
                f"""Есть в продаже в нашем магазине - {product_name}. Номера других производителей: {cross_numbers}.
        
        ✅ У нас большой ассортимент запасных частей для грузовиков по лучшим ценам в наличии и под заказ!
        📣 Звоните, пишите, подберем лучший вариант.
        🏠 Забрать запчасти для грузовых автомобилей можно в одном из наших филиалов по всей России.
        🚛 Отправка в любой город РФ транспортными компаниями.
        🚀 Бесплатная доставка по СПб в день заказа!
        
        ‼️ Работаем {work_days}, {suterday_work_time}, {sunday_work_time} ‼️"""
                for product_name, cross_numbers, work_days, suterday_work_time, sunday_work_time in tqdm.tqdm(
                    iterable=zip(
                        df_union["product_name"],
                        df_union["cross_numbers"],
                        df_union["WorkDays"],
                        df_union["SuterdayWorkTime"],
                        df_union["SundayWorkTime"],
                    ),
                    ncols=150,
                    desc=f"Работа над описанием карточки товара в мастер файле {city}",
                )
            ]

        else:
            df_union["Description"] = [
                f"""Есть в продаже в нашем магазине - {product_name}. Номера других производителей: {cross_numbers}.
            
            ✅ У нас большой ассортимент запасных частей для грузовиков по лучшим ценам в наличии и под заказ!
            📣 Звоните, пишите, подберем лучший вариант.
            🏠 Забрать запчасти для грузовых автомобилей можно в одном из наших филиалов по всей России.
            🚛 Отправка в любой город РФ транспортными компаниями.
            
            ‼️ Работаем {work_days}, {suterday_work_time}, {sunday_work_time} ‼️"""
                for product_name, cross_numbers, work_days, suterday_work_time, sunday_work_time in tqdm.tqdm(
                    iterable=zip(
                        df_union["product_name"],
                        df_union["cross_numbers"],
                        df_union["WorkDays"],
                        df_union["SuterdayWorkTime"],
                        df_union["SundayWorkTime"],
                    ),
                    ncols=150,
                    desc=f"Работа над описанием карточки товара в мастер файле {city}",
                )
            ]

        # Обновить индексы
        df_union = df_union.reset_index(drop=True)

        # Обновить индексы для создания ID avito
        df_union = df_union.reset_index()

        # Создание ID для AVITO
        df_union["avito_id"] = [
            f"{str(brand)}_{product_oem_number}"
            for brand, product_oem_number in zip(
                df_union["brand"], df_union["product_oem_number"]
            )
        ]

        # Удалить строки без цены
        df_union: pd.DataFrame = df_union[~df_union["price"].isna()]

        # Очистить номера по каталогу от лишних символов
        df_union["clear_product_catalog_number"] = [
            catalog_number_false_symbols(
                catalog_number=prod_cut_num, clearing_type="sotrans"
            )
            for prod_cut_num in tqdm.tqdm(
                iterable=df_union["product_catalog_number"],
                ncols=150,
                desc="Очистка номеров по каталогу от лишних символоы",
            )
        ]

        # Бренд в верхний регистр
        df_union["brand"] = df_union["brand"].apply(lambda x: str(x).upper())

        # Создание ключа для поиска соответствий
        df_union["key_column"] = (
            df_union["brand"] + df_union["clear_product_catalog_number"]
        )

        # Округление цены вверх
        df_union["price"] = [math.ceil(float(price)) for price in df_union["price"]]

        # Поиск и добавление ссылок на фотографии
        df_union: pd.DataFrame = df_union.merge(
            right=df_urls, how="left", on="key_column"
        )

        df_union["photo_urls"] = df_union["photo_urls"].fillna(
            "https://i.postimg.cc/2S2pVV8v/spb1.jpg"
        )

        # Смена директории
        os.chdir(PATH_TO_FOLDER_WITH_RESULT_FILES)

        # Сохранение результата
        (
            df_result.assign(
                Id=df_union["avito_id"],
                AvitoId="",
                AdStatus="",
                ManagerName=df_union["ManagerName"],
                ContactPhone=df_union["ContactPhone"],
                Address=df_union["Address"],
                Category=df_union["Category"],
                ProductType=df_union["ProductType"],
                AdType=df_union["AdType"],
                Title=df_union["product_name"],
                Description=df_union["Description"],
                Price=df_union["price"],
                Condition="новое",
                OEM=df_union["product_oem_number"],
                Cross=df_union["cross_numbers"],
                Brand=df_union["brand"],
                Availability="в наличии",
                ImageUrls=df_union["photo_urls"],
            ).to_excel(excel_writer=f"{city}.xlsx", engine="openpyxl", index=False)
        )

    else:
        print(f"Нет файлов для: {city}")
        continue

print("Файлы сохранены!")

# 5. Info
# Время, затраченное на выполнение скрипта
print(
    f"Время, затраченное на выполнение скрипта: {datetime.datetime.now() - start_time}"
)
