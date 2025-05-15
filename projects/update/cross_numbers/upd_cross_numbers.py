"""
Модуль обновления данных кросс-номеров в базе данных PostgreSQL.
"""
# ##################################################
# IMPORTS
# ##################################################
import sys

sys.path.append(r'C:\Users\user\Desktop\github\new')
from source.core.de.etl.update.cross_numbers.cross_numbers import ETLCrossNumbers


# ##################################################
# MAIN FUNCTION
# ##################################################
if __name__ == '__main__':
    etl_cross_numbers = ETLCrossNumbers()
    etl_cross_numbers.run()
