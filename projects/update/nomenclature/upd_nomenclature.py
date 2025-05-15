"""
Модуль обновления данных кросс-номеров в базе данных PostgreSQL.
"""
# ##################################################
# IMPORTS
# ##################################################
import sys

sys.path.append(r'C:\Users\user\Desktop\github\new')
from source.core.de.etl.update.nomenclature.nomenclature import ETLNomenclature


# ##################################################
# MAIN FUNCTION
# ##################################################
if __name__ == '__main__':
    etl_nomenclature = ETLNomenclature()
    etl_nomenclature.run()
