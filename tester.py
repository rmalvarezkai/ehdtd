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

import ccxt # pylint: disable=unused-import
from ehdtd import Ehdtd # pylint: disable=unused-import
from ehdtd.binance import BinanceEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.bybit import BybitEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.okx import OkxEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.kucoin import KucoinEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.bingx import BingxEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.binanceus import BinanceusEhdtdAuxClass # pylint: disable=unused-import

# from ehdtd.binance import BinanceEhdtdAuxClass
import ehdtd.ehdtd_common_functions as ecf # pylint: disable=unused-import

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False

    __aux_skel_class = BinanceEhdtdAuxClass
    __aux_test_class = BinanceusEhdtdAuxClass
    __aux_skel_inst = __aux_skel_class()
    __aux_test_inst = __aux_test_class()

    symbol = 'BTC/USDT' # pylint: disable=unused-variable
    interval = '1m' # pylint: disable=unused-variable
    limit = 14 # pylint: disable=unused-variable
    # start_time = int(time.time() - ((86400 * 365 * 3) + 0)) # pylint: disable=unused-variable
    # start_time = int(time.time() - (OkxEhdtdAuxClass.get_delta_time_from_interval(interval) * 5)) # pylint: disable=unused-variable
    # start_time = 1630454400
    end_time = int(time.time())
    end_time = end_time - ((86400 * 365 * 3) + 0)
    end_time = min(end_time, int(time.time()))
    start_time = end_time - (__aux_test_class.get_delta_time_from_interval(interval) * 5) # pylint: disable=unused-variable

    trading_type = 'SPOT' # pylint: disable=unused-variable

    start_time_out = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_time))
    end_time_out = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_time))
    print(f'start_time_out: {start_time_out}')
    print(f'end_time_out: {end_time_out}')

    post_data = None # pylint: disable=unused-variable
    headers = None # pylint: disable=unused-variable

    url = 'https://www.okx.com/api/v5/public/instruments?'
    url += 'instType=SPOT'
    # print(f'url: {url}')

    # __data = ecf.file_get_contents_url(url, 'r', post_data, headers)
    # if ecf.is_json(__data):
    #     __data = json.loads(__data)

    # pprint.pprint(__data, sort_dicts=False)
    # print('=' * 80)
    # print('')


    # __test_data = __aux_test_inst.get_last_klines_candlestick_data(symbol, interval,\
    #                                                                start_time, limit)

    __test_data = __aux_test_class.get_kline_data(symbol,\
                                                  interval,\
                                                  start_time=start_time,\
                                                  end_time=end_time,\
                                                  limit=limit)

    pprint.pprint(__test_data, sort_dicts=False)
    print('=' * 80)
    print('')

    __test_data = __aux_test_class.get_symbol_first_year_month_listed(symbol,\
                                                                      interval,\
                                                                      trading_type=trading_type)

    pprint.pprint(__test_data, sort_dicts=False)
    print('=' * 80)
    print('')

    return result

    # __skel_data = __aux_skel_inst.get_last_klines_candlestick_data(symbol,\
    #                                                                interval,\
    #                                                                start_time,\
    #                                                                limit)

    # __test_data = __aux_test_inst.get_last_klines_candlestick_data(symbol,\
    #                                                                interval,\
    #                                                                start_time,\
    #                                                                limit)

    # __skel_data = __aux_skel_inst.get_symbol_first_year_month_listed(symbol,\
    #                                                                  interval,\
    #                                                                  trading_type=trading_type)

    # pprint.pprint(__skel_data, sort_dicts=False)
    # print('+' * 80)
    # print()

    __test_data = __aux_test_inst.get_symbol_first_year_month_listed(symbol,\
                                                                     interval,\
                                                                     trading_type=trading_type)


    __year, __month = __test_data

    start_time = int(round(datetime.datetime(__year, __month, 1, 0, 0, 0, 0).timestamp()))

    print(f'INICIO: {__year} - {__month} -> {start_time}')

    __test_data = __aux_test_inst.get_last_klines_candlestick_data(symbol,\
                                                                   interval,\
                                                                   start_time,\
                                                                   limit)

    pprint.pprint(__test_data, sort_dicts=False)
    print('=' * 80)
    print()


    # pprint.pprint(__test_data, sort_dicts=False)
    # print('+' * 80)
    # print()

    # __year, __month = __test_data
    # start_time = int(round(datetime.datetime(__year, __month, 1, 0, 0, 0, 0).timestamp()))

    # __test_data = __aux_test_inst.get_last_klines_candlestick_data(symbol,\
    #                                                                interval,\
    #                                                                start_time,\
    #                                                                limit)
    # pprint.pprint(__test_data, sort_dicts=False)
    # print('+' * 80)
    # print()


    return result









if __name__ == "__main__":
    main(sys.argv[1:])
