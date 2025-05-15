import psycopg2
from psycopg2 import Error
from tqdm import tqdm

# Параметры подключения е базе данных
db_params: dict[str, str] = {
    'db_name': 'one_c',
    'user': 'postgres',
    'password': '30691',
    'host': 'localhost',
    'port': '5432'
}

# Список таблиц
tables: list[str] = [
    "bm_st_init", "bm_en_final", "bm_ex_complect", "bm_ex_decomplect",
    "bm_ex_inventory", "bm_ex_movement", "bm_ex_resort", "bm_ex_sale",
    "bm_ex_update", "bm_ex_write_off", "bm_in_complect", "bm_in_decomplect",
    "bm_in_entering", "bm_in_inventory", "bm_in_movement", "bm_in_receipt",
    "bm_in_resort", "bm_in_update", "bm_tr_transfer"
]