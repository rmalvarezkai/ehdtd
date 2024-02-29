#!/usr/bin/python
"""
Ehdtd - cryptoCurrency Exchange history data to database
Only for tester some code.

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import sys
import time # pylint: disable=unused-import
import os
import logging # pylint: disable=unused-import

import pprint # pylint: disable=unused-import

from ehdtd import Ehdtd
# from ehdtd.binance import BinanceEhdtdAuxClass
# import ehdtd.ehdtd_common_functions as ecf

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False


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
