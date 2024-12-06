#!/usr/bin/python
"""
Ehdtd - cryptoCurrency Exchange history data to database
Only for tester some code.

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import sys
import time # pylint: disable=unused-import
import os # pylint: disable=unused-import
import logging # pylint: disable=unused-import

import pprint # pylint: disable=unused-import

from ehdtd import Ehdtd # pylint: disable=unused-import
from ehdtd.binance import BinanceEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.bybit import BybitEhdtdAuxClass # pylint: disable=unused-import
# from ehdtd.binance import BinanceEhdtdAuxClass
# import ehdtd.ehdtd_common_functions as ecf

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False


    symbol = 'BTC/USDT' # pylint: disable=unused-variable
    interval = '1mo' # pylint: disable=unused-variable

    # __aux_class = BinanceEhdtdAuxClass
    __aux_class = BybitEhdtdAuxClass
    __aux_inst = __aux_class()

    # __data = __aux_class.get_exchange_connectivity()
    # pprint.pprint(__data, sort_dicts=False)

    __data = __aux_class.get_symbol_first_year_month_listed(symbol, interval)
    print(f'data: {__data}')

    # __data = __aux_inst.get_exchange_info()
    # pprint.pprint(__data, sort_dicts=False)
    # print('+' * 80)

    # __data = __aux_inst.get_exchange_full_list_symbols()
    # pprint.pprint(__data, sort_dicts=False)
    # print('+' * 80)

    # __data = __aux_class.get_kline_data(symbol, interval, limit=5)
    __data = __aux_inst.get_last_klines_candlestick_data(symbol, interval, start_time=None, limit=5)
    pprint.pprint(__data, sort_dicts=False)
    print('+' * 80)


    # Obtiene el manejador de registro raíz
    # root_logger = logging.getLogger()

    # # Obtiene todos los manejadores de log registrados
    # handlers = root_logger.handlers

    # # Busca el primer manejador de archivo (FileHandler)
    # file_handler = (
    #     next((handler for handler in handlers if isinstance(handler, logging.FileHandler)), None)
    # )

    # if file_handler:
    #     # Obtiene la ruta del archivo de registro
    #     log_file_path = file_handler.baseFilename
    #     # Obtiene el directorio del archivo de registro
    #     log_directory = os.path.dirname(log_file_path)

    #     print(f"Directorio de registro: {log_directory}")
    # else:
    #     print("No se encontró un manejador de archivo en la configuración de registro.")

    # Obtener el directorio de inicio del usuario
    # log_file_path = Ehdtd.get_default_log_file()

    # print(f"Directorio de log de ehdtd: {log_file_path}")
    return result









if __name__ == "__main__":
    main(sys.argv[1:])
