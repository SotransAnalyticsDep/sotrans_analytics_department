"""
Модуль отвечает за выбор метода получения данных из базы данных.
"""

# ##################################################
# ИМПОРТЫ
# ##################################################
from ..sql.sql_start import SQLStart

# ##################################################
# API
# ##################################################
METHODS_MAP = {
    "st": {"init": SQLStart.get_sql_query_init},
    "in": {
        "complect": None,
        "decomplect": None,
        "entering": None,
        "inventory": None,
        "movement": None,
        "receipt": None,
        "resort": None,
        "update": None,
    },
    "hv": {
        "stock": None,
        "sale": None,
        "lost_demand": None,
    },
    "ex": {
        "complect": None,
        "decomplect": None,
        "inventory": None,
        "movement": None,
        "resort": None,
        "sale": None,
        "update": None,
        "write_off": None,
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
