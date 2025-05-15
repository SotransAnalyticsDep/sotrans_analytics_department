# 1. Imports
# Встроенные библиотеки
import os
import time
import win32clipboard

# Сторонние библиотеки
import pandas as pd
import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementNotInteractableException,
)

# 2. Settings
## 2.1. Path & Links
# Путь к корневой директории
PATH_ROOT_FOLDER: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files"
PATH_TO_LOOKUPS: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files\lookups"

# Ссылка на стартовую страницу
start_page_url: str = "https://postimages.org/"

## 2.2. Constants
DICT_BRAND_URLS: dict[str, str] = {
    "AEOLUS": "https://postimg.cc/gallery/01P1tgSL",
    "ALPHALINE": "https://postimg.cc/gallery/ZtjpgXnk",
    "AUTOMANN": "https://postimg.cc/gallery/qJh5QKj",
    "BU": "https://postimg.cc/gallery/rTzNqQtX",
    "BU_1": "https://postimg.cc/gallery/WLmgQd3w",
    "BU_2": "https://postimg.cc/gallery/yT36wmF",
    "BU_3": "https://postimg.cc/gallery/ZxMMnqd",
    "BU_4": "https://postimg.cc/gallery/G1yPSxhk",
    "BU_5": "https://postimg.cc/gallery/7rkqgbV3",
    "CAT": "https://postimg.cc/gallery/15c90v1",
    "CEI": "https://postimg.cc/gallery/8gmqgM4W",
    "COJALI": "https://postimg.cc/gallery/hjsx8cD",
    "CONNECT": "https://postimg.cc/gallery/4g0Yv9Tr",
    "DETROIT DIESEL": "https://postimg.cc/gallery/H1mQRP6Y",
    "DONGFENG": "https://postimg.cc/gallery/V4JwV1N",
    "DOUBLECOIN": "https://postimg.cc/gallery/6C2F16v",
    "EUROTECH": "https://postimg.cc/gallery/V504B5x",
    "FREIGHTLINER": "https://postimg.cc/gallery/yCr8cDgM",
    "FRENBU": "https://postimg.cc/gallery/MbYNkKC",
    "FSS": "https://postimg.cc/gallery/4Rsww3yN",
    "FULLER": "https://postimg.cc/gallery/pH7254mK",
    "GLADIATOR": "https://postimg.cc/gallery/3HCcZpL6",
    "GOLDEN DRAGON_1": "https://postimg.cc/gallery/dPj1k8LJ",
    "GOLDEN DRAGON_2": "https://postimg.cc/gallery/pbZMPxSK",
    "GTSA": "https://postimg.cc/gallery/fhq4yQT",
    "HARBINGER": "https://postimg.cc/gallery/FYsY0rG",
    "HARTUNG": "https://postimg.cc/gallery/pM4XgKwk",
    "HENGST": "https://postimg.cc/gallery/Q7gp9Sk",
    "HOBI": "https://postimg.cc/gallery/32xTm3q",
    "HP": "https://postimg.cc/gallery/8DzT4t6",
    "ISIKSAN": "https://postimg.cc/gallery/yHQgj5c",
    "KALE": "https://postimg.cc/gallery/g0C2dT1",
    "KAMA": "https://postimg.cc/gallery/PksCym4n",  # Eng
    "LENKSTARK": "https://postimg.cc/gallery/PP2pqQs",
    "LUZAR": "https://postimg.cc/gallery/pH8KS8rf",
    "MANNFILTERS": "https://postimg.cc/gallery/b1ryxSZ",
    "MICHELIN": "https://postimg.cc/gallery/JChpdzJP",
    "PAI": "https://postimg.cc/gallery/HD4fVgMK",
    "ROSTAR": "https://postimg.cc/gallery/ms2j7zkT",
    "ROSTSELMASH": "https://postimg.cc/gallery/M2kRNcyJ",
    "S&S": "https://postimg.cc/gallery/DTnTJGdd",
    "SAF": "https://postimg.cc/gallery/tVZ117x",
    "SAMPA_1": "https://postimg.cc/gallery/8gGFTzSr",
    "SAMPA_2": "https://postimg.cc/gallery/vFYHwSVt",
    "SAMPIYON": "https://postimg.cc/gallery/JGpFQ0B",
    "SMARTTECH": "https://postimg.cc/gallery/9wyHghZ",
    "SOTRANS": "https://postimg.cc/gallery/tgn9SG2",
    "SOYLU": "https://postimg.cc/gallery/5H8M4zs",
    "SPUTNIK": "https://postimg.cc/gallery/m4kBrcYy",
    "STARVOLT": "https://postimg.cc/gallery/42Bhvtd",
    "STUFF": "https://postimg.cc/gallery/CVRsbZn",
    "SUNRISE": "https://postimg.cc/gallery/xzwg1c3",
    "TANGDE_1": "https://postimg.cc/gallery/qkLj0rX",
    "TANGDE_2": "https://postimg.cc/gallery/G38R8BV",
    "TECHAUTOSVET": "https://postimg.cc/gallery/5QdBYH5",
    "TRIALLI": "https://postimg.cc/gallery/GnXkZdQp",
    "TRUCKEXPERT": "https://postimg.cc/gallery/Gvx63Gv",
    "TRUCKPART": "https://postimg.cc/gallery/cKLJ6X8",
    "TSN": "https://postimg.cc/gallery/kdnjvxM",
    "TTT": "https://postimg.cc/gallery/HLR3fR0",
    "VOB": "https://postimg.cc/gallery/WvVY4cSF",
    "WABCO": "https://postimg.cc/gallery/gWjH6613",
    "WASPO": "https://postimg.cc/gallery/G8bhSdj",
    "КАМА": "https://postimg.cc/gallery/PksCym4n",  # Rus
}

## 2.3. Variables
list_with_dataframes = []

## 2.4. Pypl
# Настройки selenium
WEBDRIVER_OPTIONS = webdriver.ChromeOptions()

# WEBDRIVER_OPTIONS.add_argument('--headless')
# WEBDRIVER_OPTIONS.add_argument('--disable-gpu')

# Настройки библиотеки "Pandas"
pd.set_option("display.max_columns", None)  # Количество колонок видимое в таблице
pd.set_option("display.max_colwidth", None)  # Количество отображаемых символов в записи
pd.set_option(
    "display.float_format", "{:.2f}".format
)  # Количество десятичных знаков после запятой


## 2.5. Functions
def catalog_number_false_symbols(catalog_number: str, clearing_type: str) -> str | None:
    """Функция принимает на вход два параметра: номер, который требуется очистить и тип очистки.
    - Для "origin" предусмотрена обычная очистка от символов пунктуации.
    - Для "sotrans" предусмотрена предварительная сепарация кода.

    :catalog_number:
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


# 5. Main Program
## 5.1. Collecting urls
# Создание браузера
browser = webdriver.Chrome()
# Создание браузера
# browser = webdriver.Chrome(options=WEBDRIVER_OPTIONS, service=Service(ChromeDriverManager().install()))

# Открытие стартовой страницы
browser.get(start_page_url)

# Нажать кнопки логина
browser.find_element(
    by=By.XPATH, value="/html/body/header/div[1]/nav[2]/ul/li[1]/a"
).click()

# Ввести логин
browser.find_element(
    by=By.XPATH, value='//*[@id="content"]/div/div/div[1]/form[1]/div[1]/div[2]/input'
).send_keys("ags@agsauto.us")

# Ввести пароль
browser.find_element(
    by=By.XPATH, value='//*[@id="content"]/div/div/div[1]/form[1]/div[2]/div[2]/input'
).send_keys("vPkRztTYzDFm")

# Нажать кнопку "Вход"
browser.find_element(
    by=By.XPATH, value='//*[@id="content"]/div/div/div[1]/form[1]/div[3]/div/input'
).click()
# Цикл по папкам брендов с фотографиями
for brand_name, brand_folder_url in DICT_BRAND_URLS.items():
    # Открыть папку по ссылке
    browser.get(brand_folder_url)

    time.sleep(0.5)

    try:
        # Нажать на "Поделиться"
        browser.find_element(by=By.XPATH, value='//*[@id="collapse_share"]').click()

        time.sleep(0.5)

        # Выбрать в чек-боксе "Прямая ссылка"
        browser.find_element(
            by=By.CSS_SELECTOR, value="#embed_box > option:nth-child(2)"
        ).click()

        time.sleep(0.5)

        # Нажать на кнопку "Скопировать" (ссылки)
        browser.find_element(
            by=By.XPATH,
            value="/html/body/div[1]/div/div[1]/div[3]/div/form/div[2]/div[2]/div/span/button",
        ).click()

        time.sleep(0.5)

        # Сохранение данных в переменную из буфера обмена
        win32clipboard.OpenClipboard()

        photo_urls = (
            win32clipboard.GetClipboardData().replace("\r", " ").split(" \n")[:-1]
        )

        win32clipboard.CloseClipboard()

        time.sleep(0.5)

        # Сформировать датафрейм из ссылок на фотографии
        temp_df_photos: pd.DataFrame = pd.DataFrame(photo_urls).rename(
            columns={0: "url"}
        )

        # Добавить столбец с наименованием бренда
        temp_df_photos["brand_name"] = brand_name

        # Добавить датафрейм в список
        list_with_dataframes.append(temp_df_photos)

    except ElementNotInteractableException as err:
        print(str(err))
        continue

    except NoSuchElementException as err:
        print(str(err))
        continue

browser.quit()
# Объединить все датафреймы в один
df_photos: pd.DataFrame = pd.concat(list_with_dataframes).reset_index(drop=True)

## 5.2. Preprocessing
false_photo = []
for idx, url in enumerate(df_photos["url"]):
    if url.find("-photo-") > 0:
        df_photos.loc[idx, "product_photo_id"] = (
            url.split("-photo-")[1].replace(".jpg", "").replace(".png", "")
        )
    else:
        false_photo.append(url)
        print(url)

df_photos: pd.DataFrame = df_photos[~df_photos["url"].isin(false_photo)]
# Получить артикул из ссылки
df_photos["product_catalog_number"] = [
    photo_url.split("-photo-")[0].split("/", -1)[-1]
    for photo_url in tqdm.tqdm(iterable=df_photos["url"], desc="Получение артикула")
]

df_photos: pd.DataFrame = (
    df_photos[["brand_name", "product_catalog_number", "url", "product_photo_id"]]
    .sort_values(
        by=["brand_name", "product_catalog_number", "product_photo_id"],
        ascending=[True, True, True],
    )
    .reset_index(drop=True)
)
pt_df_photos: pd.DataFrame = pd.pivot_table(
    data=df_photos,
    index=["brand_name", "product_catalog_number"],
    columns="product_photo_id",
    values="url",
    aggfunc=",".join,
)
pt_df_photos = pt_df_photos.reset_index()
pt_df_photos["photo_urls"] = [
    f"{pic_0} | {pic_1} | {pic_2} | {pic_3} | {pic_4} | {pic_5} | {pic_6} | {pic_7} | {pic_8} | {pic_9} | {pic_10}"
    for pic_0, pic_1, pic_2, pic_3, pic_4, pic_5, pic_6, pic_7, pic_8, pic_9, pic_10 in zip(
        pt_df_photos["0"],
        pt_df_photos["1"],
        pt_df_photos["2"],
        pt_df_photos["3"],
        pt_df_photos["4"],
        pt_df_photos["5"],
        pt_df_photos["6"],
        pt_df_photos["7"],
        pt_df_photos["8"],
        pt_df_photos["9"],
        pt_df_photos["10"],
    )
]
pt_df_photos["photo_urls"] = [
    urls.replace("nan | ", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    .replace(" | nan", "")
    for urls in pt_df_photos["photo_urls"]
]
# Очистить номера по каталогу от лишних символов
pt_df_photos["product_catalog_number"] = [
    catalog_number_false_symbols(catalog_number=prod_cut_num, clearing_type="sotrans")
    for prod_cut_num in tqdm.tqdm(
        iterable=pt_df_photos["product_catalog_number"],
        ncols=150,
        desc="Очистка номеров по каталогу от лишних символоы",
    )
]

# Создание ключа для поиска соответствий
pt_df_photos["key_column"] = (
    pt_df_photos["brand_name"] + pt_df_photos["product_catalog_number"]
)
pt_df_photos = pt_df_photos[["key_column", "photo_urls"]]

# 6. Saving
# Смена директории
os.chdir(PATH_TO_LOOKUPS)
# Сохранение данных в JSON
pt_df_photos.to_json(path_or_buf="photo_urls.json")
print("Готово!")
