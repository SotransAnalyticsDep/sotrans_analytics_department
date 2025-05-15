import pandas as pd
import aiohttp
import asyncio
import os
from urllib.parse import urlparse


PATH_TO_SAVE: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files\photo"
PATH_JSON: str = r"C:\Users\user\Desktop\github\new_sotrans_analytics_department\projects\tools\avito\files\lookups\photo_urls.json"

brands: tuple[str] = (
    "AEOLUS",
    "ALPHALINE",
    "AUTOMANN",
    "BU",
    "BU_1",
    "BU_2",
    "BU_3",
    "BU_4",
    "BU_5",
    "CAT",
    "CEI",
    "COJALI",
    "CONNECT",
    "DETROIT DIESEL",
    "DONGFENG",
    "DOUBLECOIN",
    "EUROTECH",
    "FREIGHTLINER",
    "FRENBU",
    "FSS",
    "FULLER",
    "GLADIATOR",
    "GOLDEN DRAGON_1",
    "GOLDEN DRAGON_2",
    "GTSA",
    "HARBINGER",
    "HARTUNG",
    "HENGST",
    "HOBI",
    "HP",
    "ISIKSAN",
    "KALE",
    "KAMA",
    "LENKSTARK",
    "LUZAR",
    "MANNFILTERS",
    "MICHELIN",
    "PAI",
    "ROSTAR",
    "ROSTSELMASH",
    "S&S",
    "SAF",
    "SAMPA_1",
    "SAMPA_2",
    "SAMPIYON",
    "SMARTTECH",
    "SOTRANS",
    "SOYLU",
    "SPUTNIK",
    "STARVOLT",
    "STUFF",
    "SUNRISE",
    "TANGDE_1",
    "TANGDE_2",
    "TECHAUTOSVET",
    "TRIALLI",
    "TRUCKEXPERT",
    "TRUCKPART",
    "TSN",
    "TTT",
    "VOB",
    "WABCO",
    "WASPO",
    "КАМА",
)


async def download_image(session, url, folder_path):
    """Асинхронное скачивание одного изображения"""
    try:
        # Получаем имя файла из URL
        file_name = os.path.basename(urlparse(url).path)
        file_path = os.path.join(folder_path, file_name)

        # Скачиваем изображение
        async with session.get(url) as response:
            if response.status == 200:
                # Сохраняем файл
                with open(file_path, "wb") as f:
                    f.write(await response.read())
                print(f"Скачано: {file_name}")
            else:
                print(f"Ошибка {response.status} при скачивании {url}")
    except Exception as e:
        print(f"Ошибка при скачивании {url}: {e}")


async def process_brand(brand, df):
    """Обработка одного бренда"""
    # Фильтруем датафрейм
    filtered_df = df[df["key_column"].str.contains(brand, case=True)]

    # Создаем папку
    folder_path = os.path.join(PATH_TO_SAVE, brand)
    os.makedirs(folder_path, exist_ok=True)

    # Собираем все URL
    all_urls = []
    for _, row in filtered_df.iterrows():
        photo_urls = row["photo_urls"].split("|")
        all_urls.extend(url.strip() for url in photo_urls if url.strip())

    # Создаем асинхронную сессию и задачи
    async with aiohttp.ClientSession() as session:
        tasks = [download_image(session, url, folder_path) for url in all_urls]
        await asyncio.gather(*tasks)


async def main(df):
    """Главная функция для обработки первого бренда"""
    first_brand = brands[0]
    await process_brand(first_brand, df)


# Запуск для первого бренда
if __name__ == "__main__":
    df = pd.read_json(PATH_JSON)

    async def main(df):
        tasks = [process_brand(brand, df) for brand in brands]
        await asyncio.gather(*tasks)

    asyncio.run(main(df))
