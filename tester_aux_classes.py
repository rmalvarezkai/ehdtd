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
from ehdtd.okx import OkxEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.kucoin import KucoinEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.bingx import BingxEhdtdAuxClass # pylint: disable=unused-import
from ehdtd.binanceus import BinanceusEhdtdAuxClass # pylint: disable=unused-import

import ehdtd.ehdtd_common_functions as ecf

def execute_function_from_class(class_to_test, function_data):
    """
    execute_function_from_class
    ===========================
    """

    result = {
        'is_exec': False,
        'result': None
    }

    function_name = function_data['function']
    function_params = function_data.get('params', {})
    function_type = function_data['type']

    __aux_class = None

    if function_type == 'inst':
        __aux_class = class_to_test()
    elif function_type == 'class':
        __aux_class = class_to_test

    if __aux_class is not None:
        if hasattr(__aux_class, function_name):
            func = getattr(__aux_class, function_name)
            if callable(func):
                __res = func(**function_params)
                result['is_exec'] = True
                result['result'] = __res

    return result

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False

    trading_type = 'SPOT'
    sort_list = True
    symbol = 'BTC/USDT' # pylint: disable=unused-variable
    interval = '1mo' # pylint: disable=unused-variable
    interval = '1m' # pylint: disable=unused-variable
    start_time = None
    end_time = None
    limit = 1000

    __test_functions = []

    __func_add = {
        'function': 'get_exchange_full_list_symbols',
        'params': {
            'sort_list': sort_list
        },
        'type': 'inst'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'if_symbol_supported',
        'params': {
            'symbol': symbol
        },
        'type': 'inst'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_last_klines_candlestick_data',
        'params': {
            'symbol': symbol,
            'interval': interval,
            'start_time': start_time,
            'limit': limit
        },
        'type': 'inst'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_api_url',
        'params': {
            'trading_type': trading_type
        },

        'type': 'class'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_exchange_connectivity',
        'params': {},
        'type': 'class'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_symbol_from_unified_symbol',
        'params': {
            'symbol': symbol
        },
        'type': 'class'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_kline_data',
        'params': {
            'symbol': symbol,
            'interval': interval,
            'start_time': start_time,
            'end_time': end_time,
            'limit': limit,
            'trading_type': trading_type
        },
        'type': 'class'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_symbol_first_year_month_listed',
        'params': {
            'symbol': symbol,
            'interval': interval,
            'trading_type': trading_type
        },
        'type': 'class'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'not_daily_data',
        'params': {},
        'type': 'class'
    }

    __func_add = {
        'function': 'get_delta_time_from_interval',
        'params': {
            'interval': interval,
            'year': 1980,
            'month': 10
        },
        'type': 'class'
    }

    __test_functions.append(__func_add)

    __func_add = {
        'function': 'get_supported_intervals',
        'params': {},
        'type': 'class'
    }

    __test_functions.append(__func_add)

    __aux_skel_class = BinanceEhdtdAuxClass
    __aux_test_classes = [
            {
                'class_name': 'BybitEhdtdAuxClass',
                'class': BybitEhdtdAuxClass
            },
            {
                'class_name': 'OkxEhdtdAuxClass',
                'class': OkxEhdtdAuxClass
            },
            {
                'class_name': 'KucoinEhdtdAuxClass',
                'class': KucoinEhdtdAuxClass
            },
            {
                'class_name': 'BingxEhdtdAuxClass',
                'class': BingxEhdtdAuxClass
            },
            {
                'class_name': 'BinanceusEhdtdAuxClass',
                'class': BinanceusEhdtdAuxClass
            }
        ]

    for __aux_test_class in __aux_test_classes:
        # print(f'class: {__aux_test_class["class_name"]}')

        for __test_function in __test_functions:
            # pprint.pprint(__test_function, sort_dicts=False)
            # print('+' * 80)

            __result_skel_class = execute_function_from_class(__aux_skel_class, __test_function)
            __result_test_class = execute_function_from_class(__aux_test_class['class'],\
                                                              __test_function)

            if __result_skel_class['is_exec'] == __result_test_class['is_exec']\
                and ecf.compare_structure(__result_skel_class['result'],\
                                          __result_test_class['result']):
                str_out = f'class: {__aux_test_class["class_name"]},'
                str_out += f' function: {__test_function["function"]} -> OK'
                print(str_out)
            else:
                str_out = f'class: {__aux_test_class["class_name"]},'
                str_out += f' function: {__test_function["function"]} -> ERROR'
                print(str_out)
                print()

                pprint.pprint(__result_skel_class, sort_dicts=False)
                print('+' * 80)

                pprint.pprint(__result_test_class, sort_dicts=False)
                print('+' * 80)

                print('=' * 80)

    return result









if __name__ == "__main__":
    main(sys.argv[1:])
