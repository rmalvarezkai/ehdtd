#!/usr/bin/python
"""
Ehdtd - cryptoCurrency Exchange history data to database
Example use.

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import sys
import time
import os
import threading

import pprint # pylint: disable=unused-import

from ehdtd import Ehdtd
from ehdtd.binance import BinanceEhdtdAuxClass # pylint: disable=unused-import
# import ehdtd.ehdtd_common_functions as ecf

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = 1

    exchange = 'binance'
    exchange = 'bybit'

    debug = False
    get_data = True
    check_data = False
    try_fix_data = False
    # get_data = True
    # check_data = True
    # try_fix_data = True

    conn = Ehdtd.get_exchange_connectivity(exchange)

    pprint.pprint(conn, sort_dicts=False)

    stop_flag_file = '/tmp/stop_getting_data.txt'

    symbols = ['BTC/USDT', 'BNB/USDT', 'ETH/USDT', 'LTC/USDT']
    symbols = ['BTC/USDT', 'LTC/USDT']

    intervals = Ehdtd.get_supported_intervals(exchange)
    intervals = ['1m', '5m']

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

    time_wait = 14
    time_limit = 450
    time_limit = 7200

    fetch_data = []

    limit_act_threads = (len(symbols) * len(intervals)) + 2

    for interval in intervals:
        for symbol in symbols:
            fetch_data_n = {}
            fetch_data_n['symbol'] = symbol
            fetch_data_n['interval'] = interval
            fetch_data.append(fetch_data_n)

    ehd = Ehdtd(exchange, fetch_data, db_data, debug=debug)  # Create instance

    if get_data:
        ehd.start()
        time.sleep(50)

        is_stopped = False

        num_act_threads = ehd.get_num_threads_active()

        time_ini = int(round(time.time()))
        time_diff = int(round(time.time())) - time_ini
        while num_act_threads > limit_act_threads and time_diff < time_limit:
            time.sleep(time_wait)
            num_act_threads = ehd.get_num_threads_active()

            if os.path.exists(stop_flag_file):
                os.remove(stop_flag_file)
                ehd.stop()
                is_stopped = True
            time_diff = int(round(time.time())) - time_ini

        if not is_stopped:
            # date_stop = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
            ehd.stop()
            # date_stop = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
            is_stopped = True

    num_act_threads = ehd.get_num_threads_active()

    start_from = 0

    if check_data:
        fetch_data.reverse()

        for data_fetch in fetch_data:
            symbol = data_fetch['symbol']
            interval = data_fetch['interval']
            time.sleep(4)
            date_ini = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
            db_errors = ehd.check_database_data(symbol, interval, start_from)
            date_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
            if isinstance(db_errors, dict) and db_errors['result'] is not None\
                and isinstance(db_errors['result'], list):
                out_msg = f'SYMBOL: {symbol}, INTERVAL: {interval}'
                out_msg = out_msg + f', DB_ERRORS: {len(db_errors["result"])}'
                out_msg = out_msg + f' == {db_errors["result_counter"]}'
                out_msg = out_msg + f', time_proc_sec: {db_errors["time_proc_sec"]}'
                out_msg = out_msg + f', START_DATE: {date_ini}, END_DATE: {date_end}'
                # print(out_msg)
                # if len(db_errors['result']) > 0:
                #     pprint.pprint(db_errors['result'][-1], sort_dicts=False)
                # print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
            # else:
            #     out_msg = f'SYMBOL: {symbol}, INTERVAL: {interval}, CHECK ERROR'
            #     out_msg = out_msg + f', START_DATE: {date_ini}, END_DATE: {date_end}'
            #     print(out_msg)
            #     print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')

            # date_ini = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
            if try_fix_data:
                # print(f'Begin trying to fix SYMBOL: {symbol}, INTERVAL: {interval} - {date_ini}')
                ehd.try_to_fix_database_data(db_errors['result'])
                #date_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
                # print(f'End trying to fix SYMBOL: {symbol}, INTERVAL: {interval} - {date_end}')
                # print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')

            time.sleep(4)
            # date_ini = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))
            db_errors = ehd.check_database_data(symbol, interval, start_from)
            # date_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(round(time.time()))))

            # if isinstance(db_errors, dict) and db_errors['result'] is not None\
            #     and isinstance(db_errors['result'], list):
            #     out_msg = f'SYMBOL: {symbol}, INTERVAL: {interval}'
            #     out_msg = out_msg + f', DB_ERRORS: {len(db_errors["result"])}'
            #     out_msg = out_msg + f' == {db_errors["result_counter"]}'
            #     out_msg = out_msg + f', time_proc_sec: {db_errors["time_proc_sec"]}'
            #     out_msg = out_msg + f', START_DATE: {date_ini}, END_DATE: {date_end}'
            #     print(out_msg)
            #     if len(db_errors['result']) > 0:
            #         pprint.pprint(db_errors['result'][-1], sort_dicts=False)
            #     print('=========================================================================')
            # else:
            #     out_msg = f'SYMBOL: {symbol}, INTERVAL: {interval}, CHECK ERROR'
            #     out_msg = out_msg + f', START_DATE: {date_ini}, END_DATE: {date_end}'
            #     print(out_msg)
            #     print('=========================================================================')

    wait_th = True
    wait_limit = 9
    wait_counter = 0

    while wait_th:
        num_threads_active = int(threading.active_count())
        # print('num_threads_active: ' + str(num_threads_active))
        # for thread in threading.enumerate():
        #     print(f'thread_name: {thread.getName()}')

        if num_threads_active <= 3:
            wait_th = False
        if wait_counter > wait_limit:
            wait_th = False
        wait_counter += 1
        time.sleep(9)

    current_time = int(round(time.time()))
    symbol = 'BTC/USDT'
    interval = '1m'
    start_from = current_time - 7200
    start_from = 0
    until_to = current_time
    return_type = 'pandas'
    return_type = 'list'
    return_type = 'list_consistent_streams'
    return_type = 'list_consistent_streams_pandas'

    data_db = ehd.get_data_from_db(symbol, interval, start_from, until_to, return_type)
    # pprint.pprint(data_db)
    # print(data_db)

    for i, data in enumerate(data_db):
        print(f'{i} -> {type(data)} -> {len(data)}')

    print(data_db)

    return result

if __name__ == "__main__":
    main(sys.argv[1:])
