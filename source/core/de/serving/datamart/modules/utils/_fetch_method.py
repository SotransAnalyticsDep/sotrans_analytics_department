"""
Модуль отвечает за выбор метода получения данных из базы данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from ..sql import (
    SQLStart,
    SQLIncome,
    SQLExpend,
    SQLEnd,
)

# ##################################################
# API
# ##################################################
METHODS_MAP = {
    "st": {"init": SQLStart.get_sql_query_init},
    "in": {
        "complect": SQLIncome.get_sql_query_complect,
        "decomplect": SQLIncome.get_sql_query_decomplect,
        "entering": SQLIncome.get_sql_query_entering,
        "inventory": SQLIncome.get_sql_query_inventory,
        "movement": SQLIncome.get_sql_query_movement,
        "receipt": SQLIncome.get_sql_query_receipt,
        "resort": SQLIncome.get_sql_query_resort,
        "update": SQLIncome.get_sql_query_update,
    },
    "hv": {
        "stock": None,
        "sale": None,
        "lost_demand": None,
    },
    "ex": {
        "complect": SQLExpend.get_sql_query_complect,
        "decomplect": SQLExpend.get_sql_query_decomplect,
        "inventory": SQLExpend.get_sql_query_inventory,
        "movement": SQLExpend.get_sql_query_movement,
        "resort": SQLExpend.get_sql_query_resort,
        "sale": SQLExpend.get_sql_query_sale,
        "update": SQLExpend.get_sql_query_update,
        "write_off": SQLExpend.get_sql_query_write_off,
    },
    "en": {
        "final": None,
    },
    "tr": {
        "transfer": None,
    },
    "sl": {
        "sale": None,
    },
    "ld": {
        "lost_demand": None,
    },
}
