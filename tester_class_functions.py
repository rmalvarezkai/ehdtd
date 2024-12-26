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

def execute_function_from_class(function_data):
    """
    execute_function_from_class
    ===========================
    """

    result = {
        'is_exec': False,
        'result': None,
        'result_msg': None
    }

    function_exchange = None
    function_name = None
    function_params = None
    __aux_class = None
    function_type = None # pylint: disable=unused-variable

    try:
        function_exchange = function_data['exchange']
        function_name = function_data['function']
        function_params = function_data.get('params', {})
        __aux_class = function_data['class']
        function_type = function_data['type'] # pylint: disable=unused-variable
        __res = None

        if __aux_class is not None:
            if hasattr(__aux_class, function_name):
                func = getattr(__aux_class, function_name)
                if callable(func):
                    __res = func(**function_params)
                    result['is_exec'] = True
                    result['result'] = __res
                    result['result_msg'] = None

    except Exception as exc: # pylint: disable=broad-except
        result['is_exec'] = False
        result['result'] = __res
        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
        err_msg = '|' * 80
        err_msg += '\n'
        err_msg += f'Found error in {__l_function}\n\texchange: {function_exchange}\n'
        err_msg += f'function_name: {function_name}\n\tfunction_type: {function_type}\n'
        err_msg += f'function_params: {function_params}\n\terror: {exc}\n'
        err_msg += '|' * 80
        err_msg += '\n'
        result['result_msg'] = err_msg
        # print(err_msg)
        time.sleep(5)

    return result

def get_functions_to_test(class_meths, class_inst, exchange, symbol, interval):
    """
    get_functions_to_test
    =====================
    """
    result = {}

    function_to_exec = 'get_symbol_first_year_month_listed'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'get_first_open_time_in_db'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'get_last_open_time_in_db'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'get_last_close_time_in_db'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'get_min_unchecked_open_time_in_db'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'get_min_valid_historical_open_time'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'check_and_fix_database_data'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'last_n_values': 9
        },
        'class': class_inst,
        'type': 'inst'
    }

    function_to_exec = 'get_last_klines_candlestick_data'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'exchange': exchange,
            'symbol': symbol,
            'interval': interval,
            'start_time': None,
            'limit': 5
        },
        'class': class_meths,
        'type': 'class'
    }

    function_to_exec = 'get_supported_exchanges'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
        },
        'class': class_meths,
        'type': 'class'
    }

    function_to_exec = 'get_exchange_connectivity'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'exchange': exchange
        },
        'class': class_meths,
        'type': 'class'
    }

    function_to_exec = 'get_supported_intervals'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'exchange': exchange
        },
        'class': class_meths,
        'type': 'class'
    }

    function_to_exec = 'not_daily_data'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'exchange': exchange
        },
        'class': class_meths,
        'type': 'class'
    }

    function_to_exec = 'get_delta_seconds_for_interval'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'interval': interval
        },
        'class': class_meths,
        'type': 'class'
    }

    function_to_exec = 'get_websocket_kline_current_data'

    result[function_to_exec] = {
        'exchange': exchange,
        'function': function_to_exec,
        'params': {
            'symbol': symbol,
            'interval': interval,
            'last_n_values': 5
        },
        'class': class_inst,
        'type': 'inst'
    }

    return result

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False

    db_data = {}
    #db_data['db_type'] = 'mysql' # postgresql, mysql
    db_data['db_type'] = 'postgresql' # postgresql, mysql
    db_data['db_name'] = 'ehdtd'
    db_data['db_user'] = 'ehdtd'
    db_data['db_pass'] = 'ehdtd_9898'
    db_data['db_host'] = '127.0.0.1'
    db_data['db_port'] = '5432'

    if db_data['db_type'] == 'mysql':
        db_data['db_port'] = '3306'

    __exchange_skel = 'binance'
    __exchanges_test = ['bybit', 'okx', 'kucoin', 'bingx', 'binanceus']

    symbol = 'BTC/USDT' # pylint: disable=unused-variable
    interval = '1mo' # pylint: disable=unused-variable
    interval = '1m' # pylint: disable=unused-variable
    debug = False

    fetch_data = []
    fetch_data_n = {}
    fetch_data_n['symbol'] = symbol
    fetch_data_n['interval'] = interval
    fetch_data.append(fetch_data_n)

    ehdtd_skel_inst = Ehdtd(__exchange_skel, fetch_data, db_data, debug=debug)  # Create instance
    ehdtd_skel_inst.start()
    ehdtd_skel_class = Ehdtd

    __skel_functions = get_functions_to_test(ehdtd_skel_class,\
                                             ehdtd_skel_inst,\
                                             __exchange_skel,\
                                             symbol,\
                                             interval)

    time.sleep(32)

    for __exchange_test in __exchanges_test:
        ehdtd_test_inst = Ehdtd(__exchange_test, fetch_data, db_data, debug=debug)
        ehdtd_test_inst.start()
        time.sleep(32)

        __test_functions = get_functions_to_test(ehdtd_skel_class,\
                                                 ehdtd_test_inst,\
                                                 __exchange_test,\
                                                 symbol,\
                                                 interval)

        for __test_func_name, __test_function in __test_functions.items():
            # pprint.pprint(__test_function, sort_dicts=False)
            # print('+' * 80)

            __result_skel_class = execute_function_from_class(__skel_functions[__test_func_name])
            __result_test_class = execute_function_from_class(__test_function)

            if __result_skel_class['is_exec']\
                and __result_skel_class['is_exec'] == __result_test_class['is_exec']\
                and ecf.compare_structure(__result_skel_class['result'],\
                                          __result_test_class['result']):
                str_out = f'exchange: {__exchange_test},'
                str_out += f' function: {__test_function["function"]} -> OK'
                print(str_out)
            else:
                print('=' * 80)
                str_out = f'exchange: {__exchange_test},'
                str_out += f' function: {__test_function["function"]} -> ERROR'
                print(str_out)

                pprint.pprint(__result_skel_class, sort_dicts=False)
                print('+' * 80)

                pprint.pprint(__result_test_class, sort_dicts=False)
                print('+' * 80)

                print('=' * 80)

        print('=' * 80)
        print()

        ehdtd_test_inst.stop()
        ehdtd_test_inst = None

    ehdtd_skel_inst.stop()

    return result

if __name__ == "__main__":
    main(sys.argv[1:])
