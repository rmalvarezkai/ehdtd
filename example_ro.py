#!/usr/bin/python
"""
Ehdtd - cryptoCurrency Exchange history data to database
Example use RO.

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import sys
import time # pylint: disable=unused-import

import pprint # pylint: disable=unused-import

from ehdtd import Ehdtd, EhdtdRO
from ehdtd.binance import BinanceEhdtdAuxClass # pylint: disable=unused-import
# import ehdtd.ehdtd_common_functions as ecf

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = 1

    exchange = 'binance'

    debug = False

    conn = Ehdtd.get_exchange_connectivity(exchange)

    pprint.pprint(conn, sort_dicts=False)


    symbols = ['BTC/USDT', 'BNB/USDT', 'ETH/USDT', 'LTC/USDT']

    intervals = Ehdtd.get_supported_intervals(exchange)
    intervals = ['1m', '3m', '5m', '15m', '3d']

    db_data = {}
    #db_data['db_type'] = 'mysql' # postgresql, mysql
    db_data['db_type'] = 'postgresql' # postgresql, mysql
    db_data['db_name'] = 'ehdtd'
    db_data['db_user'] = 'ehdtd'
    db_data['db_pass'] = 'ehdtd_9898'
    db_data['db_host'] = '127.0.0.1'

    # db_data['db_pass'] = ''
    # db_data['db_host'] = ''

    db_data['db_port'] = '5432'

    if db_data['db_type'] == 'mysql':
        db_data['db_port'] = '3306'

    ehd_ro = EhdtdRO(exchange, db_data, debug=debug)  # Create instance

    for interval in intervals:
        for symbol in symbols:
            str_out = f'Exists symbol, interval in db: {symbol}, {interval} -> '
            str_out += f'{ehd_ro.check_if_exists_symbol_interval(symbol, interval)}'
            print(str_out)

    symbol = 'BTC/USDT'
    interval = '15m'
    start_from = int(round(time.time())) - (1400 * 20)
    until_to = None
    return_type = 'list'
    # return_type = 'list_consistent_streams'

    data = ehd_ro.get_data_from_db(symbol,\
                                   interval,\
                                   start_from,\
                                   until_to,\
                                   return_type)

    print(data)

    return result

if __name__ == "__main__":
    main(sys.argv[1:])
