#!/usr/bin/python
"""
Ehdtd - cryptoCurrency Exchange history data to database
Only for tester some code.

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import sys
import time # pylint: disable=unused-import
import datetime # pylint: disable=unused-import
import os # pylint: disable=unused-import
import logging # pylint: disable=unused-import
import json # pylint: disable=unused-import

import pprint # pylint: disable=unused-import
from ccxw import Ccxw # pylint: disable=unused-import
import ccxt # pylint: disable=unused-import
from ehdtd import Ehdtd # pylint: disable=unused-import
from ehdtd.binance import BinanceEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.bybit import BybitEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.okx import OkxEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.kucoin import KucoinEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.bingx import BingxEhdtdAuxClass # pylint: disable=unused-import

# from ehdtd.binance import BinanceEhdtdAuxClass
import ehdtd.ehdtd_common_functions as ecf # pylint: disable=unused-import

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False

    exchange = 'bingx'
    symbol = 'BTC/USDT' # pylint: disable=unused-variable
    interval = '1m' # pylint: disable=unused-variable
    limit = 14 # pylint: disable=unused-variable

    trading_type = 'SPOT' # pylint: disable=unused-variable
    start_time = time.time()
    end_time = time.time()

    start_time_out = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_time))
    end_time_out = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_time))
    print(f'start_time_out: {start_time_out}')
    print(f'end_time_out: {end_time_out}')

    streams = [\
                    {\
                        'endpoint': 'order_book',\
                        'symbol': symbol
                    },\
                    {\
                        'endpoint': 'kline',\
                        'symbol': symbol,\
                        'interval': interval\
                    },\
                    {\
                        'endpoint': 'trades',\
                        'symbol': symbol
                    },\
                    {\
                        'endpoint': 'ticker',\
                        'symbol': symbol
                    }\
            ]

    streams = [
                    # {
                    #     'endpoint': 'kline',
                    #     'symbol': symbol,
                    #     'interval': interval
                    # },
                    # {
                    #     'endpoint': 'kline',
                    #     'symbol': symbol,
                    #     'interval': '5m'
                    # },
                    {
                        'endpoint': 'kline',
                        'symbol': symbol,
                        'interval': '15m'
                    }
            ]

    wsm = Ccxw(exchange,\
               streams,\
               result_max_len=3,\
               data_max_len=10)

    wsm.start()  # Start getting data

    time.sleep(2)  # Wait for available data

    for _ in range(0, 10):
        for stream in streams:
            interval = 'none'
            if 'interval' in stream:
                interval = stream['interval']
            data = wsm.get_current_data(stream['endpoint'], stream['symbol'], interval)
            pprint.pprint(data, sort_dicts=False)
            print('----------------------------------')
            time.sleep(1)
        print('============================================================')
        time.sleep(5)

    wsm.stop()  # Stop getting data

    return result









if __name__ == "__main__":
    main(sys.argv[1:])
