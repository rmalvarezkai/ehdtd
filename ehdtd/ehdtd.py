# pylint: disable=too-many-lines
"""
Ehdtd - cryptoCurrency Exchange history data to database
Main Class Ehdtd

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import logging.handlers
import os
import sys
import json
import threading
import time
import base64
import queue
import datetime
import calendar
import hashlib
import logging
import pprint # pylint: disable=unused-import
import schedule
import sqlalchemy
import pandas
from ccxw import Ccxw

import ehdtd.ehdtd_common_functions as ecf
from ehdtd.binance import BinanceEhdtdAuxClass
from ehdtd.bybit import BybitEhdtdAuxClass
from ehdtd.okx import OkxEhdtdAuxClass
from ehdtd.kucoin import KucoinEhdtdAuxClass
from ehdtd.bingx import BingxEhdtdAuxClass
from ehdtd.binanceus import BinanceusEhdtdAuxClass

class EhdtdExchangeConfig:
    """
    A configuration class for managing exchange classes.

    Attributes:
    exchange_classes (dict): A dictionary mapping exchange names to their corresponding classes.

    Example:
    To obtain the class for the 'binance' exchange use,
    EhdtdExchangeConfig.exchange_classes['binance'].
    """

    exchange_classes = {
        'binance': BinanceEhdtdAuxClass,
        'bybit': BybitEhdtdAuxClass,
        'okx': OkxEhdtdAuxClass,
        'kucoin': KucoinEhdtdAuxClass,
        'bingx': BingxEhdtdAuxClass,
        'binanceus': BinanceusEhdtdAuxClass
    }

class Ehdtd(): # pylint: disable=too-many-instance-attributes
    """
    Ehdtd - cryptoCurrency Exchange history data to database
    ========================================================
    This class retrieves historical data from exchanges and stores it in a database.

    Example:
    ```python
    import time
    from ehdtd import Ehdtd

    exchange = 'binance'
    symbol = 'BTC/USDT'
    interval = '1m'
    limit = 10

    db_data = {
        'db_type': 'postgresql',  # postgresql, mysql
        'db_name': 'ehdtd',
        'db_user': 'ehdtd',
        'db_pass': 'xxxxxxxxx',
        'db_host': '127.0.0.1',
        'db_port': '5432'
    }

    fetch_data = [
        {
            'symbol': symbol,
            'interval': interval
        }
    ]

    ehd = Ehdtd(exchange, fetch_data, db_data)  # Create an instance
    ehd.start()  # Start fetching data

    time.sleep(900)  # First time Wait for available data, for the data to be updated,
                     # you must wait between 90 minutes and 2.5 hours depending on the interval

    for v in fetch_data:
        symbol = v['symbol']
        interval = v['interval']
        start_from = 0
        until_to = None
        return_type = 'pandas'
        data_db = ehd.get_data_from_db(symbol, interval, start_from, until_to, return_type)
        print(data_db)
        print('=========================================================================')
        print('')
        time.sleep(1)

    ehd.stop()  # Stop fetching data
    ```

    How It Works:
    -------------
    For Binance:
    1. Try to retrieve data from a file. Check this link:\
        [Binance Public Data](https://github.com/binance/binance-public-data/#trades-1)
    2. If the file is not available, try to retrieve data from the API.
    3. Then get data from the WebSocket API using the Ccxw class.

    Database Columns:
    -----------------
    - `open_time`, `open_date`, `open_price`, `close_time`, `close_date`, `close_price`, `low`,
      `high`, `volume`, `exchange`, `symbol`, `interval`, `status`, `data`

    - Column `data` is not used,\
        and column `status` can have three values: `'__NON_CHECK__'`, `'__OK__'`, `'__ERROR__'`.
      - If `status == '__OK__'`, the file has consistent data.
      - If `status == '__ERROR__'`, the file has inconsistent data.
      - If `status == '__NON_CHECK__'`, the file is not analyzed.

    Retrieving Data from Database:
    ------------------------------
    Use the function `ehd.get_data_from_db(symbol, interval, start_from, until_to, return_type)`:
    - If `return_type == 'pandas'`, it returns a Pandas DataFrame.
    - If `return_type == 'list'`, it returns a list of dictionaries.

    """

    def __init__(self, exchange, fetch_data: list[dict],\
                 db_data: dict, log_dir: str=None, trading_type: str='SPOT', debug: bool=False): # pylint: disable=too-many-instance-attributes, too-many-branches, too-many-statements, too-many-arguments
        """
        Ehdtd constructor
        =================
            :param self: Ehdtd instance.
            :param exchange: str exchange name.
            :param fetch_data: list[dict]
                                    dicts must have this struct.
                                        {
                                            'symbol': 'BTC/USDT',
                                            'interval': '1d'
                                        }

                                            symbol: str valid symbol for exchange name.
                                            interval: str '1m', '3m', '5m', '15m', '30m',
                                                        '1h', '2h', '4h', '6h', '8h',
                                                        '12h', '1d', '3d', '1w', '1mo'

            :param db_data: dict
                                dict must have dist struct
                                    {
                                        db_type: str, #Only supported 'postgresql' and 'mysql'.
                                        db_name: str, #Database name.
                                        db_user: str, #Database user.
                                        db_pass: str, #Database pass.
                                        db_host: str, #Database host.
                                        db_port: str  #Database port.
                                    }

            :param log_dir: str
            :param trading_type: str only allowed 'SPOT'
            :param debug: bool

            :return: Return a new instance of the Class Ehdtd.
        """

        self.__exchange = None

        if exchange in Ehdtd.get_supported_exchanges():
            self.__exchange = exchange
        else:
            raise ValueError(f'The exchange {exchange} is not supported.')

        self.__log_logger = None
        self.__err_logger = None

        self.__log_logger_file = None
        self.__err_logger_file = None

        self.__debug = debug

        self.__current_ehdtd_thread = threading.current_thread()

        if self.__current_ehdtd_thread is not None:
            self.__current_ehdtd_thread.name = 'class_ehdtd_thread'

        logging.getLogger().addHandler(logging.NullHandler())

        self.log_enabled = self.__init_loggin_out(log_dir=log_dir)

        self.__not_partition_intervals = ['1d', '3d', '1w', '1mo']

        self.__db_type = None
        self.__db_name = None
        self.__fetch_data = None
        self.__trading_type = None

        self.__lock_thread_get_file = threading.Lock()
        self.__lock_schedule = threading.Lock()

        self.__cache_table_name = 'ehdtd_cache_data'

        if trading_type is not None\
            and isinstance(trading_type, str)\
            and trading_type in Ehdtd.get_supported_trading_types():
            self.__trading_type = trading_type
        else:
            raise ValueError(f'The trading type {trading_type} is not supported.')

        self.__exchange_aux_class = EhdtdExchangeConfig.exchange_classes[self.__exchange]()

        self.__chk_db_all_symbols_thd = {}

        self.__db_uri = None
        self.__db_engine = None
        self.__db_conn = None
        self.__db_metadata = None
        self.__primary_key_col = 'open_time'

        self.__signals_queue_gets = {}
        self.__signals_queue_checks = {}

        self.__ccxw_class = None

        if isinstance(db_data, dict) and all(key in db_data for key in\
            ['db_type', 'db_name', 'db_user', 'db_pass', 'db_host', 'db_port']):

            if all(isinstance(db_data[key], str) for key in\
                   ['db_type', 'db_name', 'db_user', 'db_pass', 'db_host', 'db_port']):
                self.__db_type = db_data['db_type']
                self.__db_name = db_data['db_name']

                if db_data['db_type'] in Ehdtd.get_supported_databases():
                    self.__db_type = db_data['db_type']
                else:
                    raise ValueError('The database type '\
                                     + str(db_data['db_type'])\
                                     + ' is not supported.')

                self.__db_uri = db_data['db_type'] + '://' + db_data['db_user'] + ':'\
                    + db_data['db_pass'] + '@' + db_data['db_host'] + ':'\
                    + db_data['db_port'] + '/' + db_data['db_name']

                if db_data['db_type'] == 'mysql':
                    self.__db_uri = db_data['db_type'] + '+pymysql://'\
                        + db_data['db_user'] + ':'\
                        + db_data['db_pass'] + '@' + db_data['db_host'] + ':'\
                        + db_data['db_port'] + '/' + db_data['db_name']

                try:
                    self.__db_engine = sqlalchemy.create_engine(self.__db_uri)

                    self.__db_conn = self.__db_engine.connect()
                    self.__db_metadata = sqlalchemy.MetaData()
                    self.__db_metadata.reflect(bind=self.__db_conn)

                except Exception as exc: # pylint: disable=broad-except
                    err_msg = 'Error on create Ehdtd instance' + str(exc)
                    print(err_msg)

                try:
                    __streams = []

                    for fetch_data_node in fetch_data:
                        __stream = {}
                        __stream['endpoint'] = 'kline'
                        __stream['symbol'] = fetch_data_node['symbol']
                        __stream['interval'] = fetch_data_node['interval']
                        __streams.append(__stream)

                    __data_max_len = 40

                    self.__ccxw_class = Ccxw(self.__exchange,\
                                             __streams,\
                                             data_max_len=__data_max_len,\
                                             result_max_len=__data_max_len,\
                                             debug=self.__debug)

                except Exception as exc: # pylint: disable=broad-except
                    err_msg = f'Error on create Ccxw instance {exc}'
                    print(err_msg)

        if self.check_fetch_data_struct(fetch_data):

            if self.__db_uri is not None:
                try:
                    __total_conns = len(fetch_data) + 5
                    __total_overflow = 2 * __total_conns
                    self.__db_engine = sqlalchemy.create_engine(self.__db_uri,\
                                                                max_overflow=__total_overflow,\
                                                                pool_size=__total_conns,\
                                                                pool_pre_ping=True,\
                                                                poolclass=None)
                    self.__db_conn = self.__db_engine.connect()
                    self.__db_metadata = sqlalchemy.MetaData()
                    self.__db_metadata.reflect(bind=self.__db_conn)

                except Exception as exc: # pylint: disable=broad-except
                    err_msg = 'Error on create Ehdtd instance' + str(exc)
                    print(err_msg)

            if self.__create_cache_table():

                self.__fetch_data = []

                for __f_data in fetch_data:
                    __add_f_data = __f_data
                    __add_f_data['last_year_in_db'], __add_f_data['last_month_in_db'] = (
                        self.get_symbol_first_year_month_listed(\
                            __f_data['symbol'],\
                            __f_data['interval'])
                    )

                    self.__fetch_data.append(__add_f_data)
            else:
                raise ValueError('Database problem.')

        else:
            raise ValueError('fetch_data is invalid.')


        for __f_data in self.__fetch_data:
            __table_name = self.__get_table_name(__f_data['symbol'], __f_data['interval'])

            self.__signals_queue_gets[__table_name] = queue.Queue()
            self.__signals_queue_checks[__table_name] = queue.Queue()
            self.__chk_db_all_symbols_thd[__table_name] = None

            if self.__db_metadata is not None and __table_name not in self.__db_metadata.tables:
                if not self.__create_klines_table(__f_data['symbol'], __f_data['interval']):
                    raise ValueError('Create table problem.')

            self.__db_table_partition_test_and_create(__f_data['symbol'],\
                                                      __f_data['interval'])

        self.__chk_db_all_symbols_thd['__ALL__'] = None

        self.__signals_queue_gets['__MAIN_THREAD__'] = queue.Queue()

        self.__main_thread = None
        self.__threads = None
        self.__is_threads_running = False
        self.__stop_running = False

    def __del__(self):
        if hasattr(self, '__is_threads_running')\
            and self.__is_threads_running\
            and not self.__stop_running:
            self.stop()

        __limit_time = 30
        __limit_counter = 0

        while hasattr(self, '__is_threads_running')\
            and self.__is_threads_running\
            and __limit_counter <= __limit_time:
            __limit_counter = __limit_counter + 1
            time.sleep(1)

        self.__db_conn.close()

    def __get_table_name(self, symbol, interval):
        result = f"{self.__exchange}__"
        result += f"{self.__trading_type.replace('/','_').replace('-','_').lower()}"
        result += f"__{symbol.replace('/','_').lower()}__{interval}"
        return result

    def __create_cache_table(self):
        result = False
        try:
            if self.__db_metadata is not None:

                if self.__cache_table_name in self.__db_metadata.tables:
                    result = True
                else:
                    __new_table = None
                    __new_table = sqlalchemy.Table(
                        self.__cache_table_name,
                        self.__db_metadata,
                        sqlalchemy.Column('key_sha256_hash',\
                                          sqlalchemy.String(64),\
                                          primary_key=True,\
                                          unique=True,\
                                          nullable=False),
                        sqlalchemy.Column('data', sqlalchemy.Text),
                        sqlalchemy.Column('extra_data', sqlalchemy.Text)
                        )

                    if __new_table is not None:
                        __new_table.create(self.__db_conn, checkfirst=True)
                        self.__db_conn.commit()
                        result = True
            else:
                result = False

        except Exception as exc: # pylint: disable=broad-except
            print('EXC: ' + str(exc))
            result = False

        return result

    def __create_klines_table(self, symbol, interval):
        result = False
        table_name = self.__get_table_name(symbol, interval)

        try:
            __new_table = None

            if self.__db_type == 'postgresql':
                if interval in self.__not_partition_intervals:
                    __new_table = sqlalchemy.Table(
                        table_name,
                        self.__db_metadata,
                        sqlalchemy.Column('open_time', sqlalchemy.BigInteger, primary_key=True,\
                                        unique=True, nullable=False),
                        sqlalchemy.Column('open_date', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('open_price', sqlalchemy.String(64), nullable=False),
                        sqlalchemy.Column('close_time', sqlalchemy.BigInteger, nullable=False),
                        sqlalchemy.Column('close_date', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('close_price', sqlalchemy.String(64), nullable=False),
                        sqlalchemy.Column('low', sqlalchemy.String(64)),
                        sqlalchemy.Column('high', sqlalchemy.String(64)),
                        sqlalchemy.Column('volume', sqlalchemy.String(64)),
                        sqlalchemy.Column('exchange', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('symbol', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('interval', sqlalchemy.String(32),\
                                        nullable=False),
                        sqlalchemy.Column('status', sqlalchemy.String(32),\
                                        server_default='__NON_CHECK__'),
                        sqlalchemy.Column('data', sqlalchemy.Text)
                        )

                else:
                    __new_table = sqlalchemy.Table(
                        table_name,
                        self.__db_metadata,
                        sqlalchemy.Column('open_time', sqlalchemy.BigInteger, primary_key=True,\
                                        unique=True, nullable=False),
                        sqlalchemy.Column('open_date', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('open_price', sqlalchemy.String(64), nullable=False),
                        sqlalchemy.Column('close_time', sqlalchemy.BigInteger, nullable=False),
                        sqlalchemy.Column('close_date', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('close_price', sqlalchemy.String(64), nullable=False),
                        sqlalchemy.Column('low', sqlalchemy.String(64)),
                        sqlalchemy.Column('high', sqlalchemy.String(64)),
                        sqlalchemy.Column('volume', sqlalchemy.String(64)),
                        sqlalchemy.Column('exchange', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('symbol', sqlalchemy.String(32), nullable=False),
                        sqlalchemy.Column('interval', sqlalchemy.String(32),\
                                        nullable=False),
                        sqlalchemy.Column('status', sqlalchemy.String(32),\
                                        server_default='__NON_CHECK__'),
                        sqlalchemy.Column('data', sqlalchemy.Text),
                        postgresql_partition_by='RANGE (open_time)'
                        )
            elif self.__db_type == 'mysql':
                __new_table = sqlalchemy.Table(
                    table_name,
                    self.__db_metadata,
                    sqlalchemy.Column('open_time', sqlalchemy.BigInteger, primary_key=True,\
                                      unique=True, nullable=False),
                    sqlalchemy.Column('open_date', sqlalchemy.String(32), nullable=False),
                    sqlalchemy.Column('open_price', sqlalchemy.String(64), nullable=False),
                    sqlalchemy.Column('close_time', sqlalchemy.BigInteger, nullable=False),
                    sqlalchemy.Column('close_date', sqlalchemy.String(32), nullable=False),
                    sqlalchemy.Column('close_price', sqlalchemy.String(64), nullable=False),
                    sqlalchemy.Column('low', sqlalchemy.String(64)),
                    sqlalchemy.Column('high', sqlalchemy.String(64)),
                    sqlalchemy.Column('volume', sqlalchemy.String(64)),
                    sqlalchemy.Column('exchange', sqlalchemy.String(32), nullable=False),
                    sqlalchemy.Column('symbol', sqlalchemy.String(32), nullable=False),
                    sqlalchemy.Column('interval', sqlalchemy.String(32),\
                                      nullable=False),
                    sqlalchemy.Column('status', sqlalchemy.String(32),\
                                      server_default='__NON_CHECK__'),
                    sqlalchemy.Column('data', sqlalchemy.Text)
                    )


            if __new_table is not None:
                __new_table.create(self.__db_conn, checkfirst=True)

                self.__db_conn.commit()

            result = True
        except Exception as exc: # pylint: disable=broad-except
            print('EXC: ' + str(exc))
            result = False

        return result

    def __db_table_partition_test_and_create(self, symbol, interval): # pylint: disable=too-many-locals
        result = False

        if interval in self.__not_partition_intervals:
            return True

        table_name = self.__get_table_name(symbol, interval)

        msg_out = f'BEGIN TEST AND CREATE PARTITION TABLE, exchange: {self.__exchange}'
        msg_out += f', symbol: {symbol}, interval: {interval}, table_name: {table_name}'
        self.__log_logger.info(msg_out)

        __min_year, __min_month = (
            self.get_symbol_first_year_month_listed(symbol, interval)
        )


        __this_time = round(time.time())
        __top_year = int(time.strftime("%Y",time.gmtime(__this_time))) + 2

        for year in range(__min_year, __top_year):
            sql_query = None
            partition_name = self.__get_table_partition_name(symbol, interval, year)
            exists_partition = (
                self.__check_if_exists_table_partition(symbol, interval, year)
            )

            next_year = year + 1

            time_ini = int(datetime.datetime(year, 1, 1, 0, 0, 0,\
                                             tzinfo=datetime.timezone.utc).timestamp())
            time_end = int(datetime.datetime(next_year, 1, 1, 0, 0, 0,\
                                             tzinfo=datetime.timezone.utc).timestamp())

            if exists_partition is not None and not exists_partition:
                if self.__db_type == 'postgresql':

                    sql_query = "CREATE TABLE " + str(partition_name)\
                        + " PARTITION OF " + str(table_name)\
                        + " FOR VALUES FROM (" + str(time_ini) + ") TO (" + str(time_end) + ") ;"

                elif self.__db_type == 'mysql':
                    partitions_data = ""
                    for year_part in range(__min_year, year + 1):
                        next_year_part = year_part + 1
                        time_end_part = int(datetime.datetime(next_year_part, 1, 1, 0, 0, 0,\
                                                        tzinfo=datetime.timezone.utc).timestamp())
                        partition_name_part = self.__get_table_partition_name(symbol,\
                                                                              interval,\
                                                                              year_part)

                        partitions_add = "PARTITION " + str(partition_name_part)\
                            + " VALUES LESS THAN (" + str(time_end_part) + ")"

                        if len(partitions_data) > 0:
                            partitions_data = partitions_data + ", " + partitions_add
                        else:
                            partitions_data = partitions_data + partitions_add

                    sql_query = "ALTER TABLE " + str(table_name)\
                        + " PARTITION BY RANGE(open_time) (" + str(partitions_data) + ") ;"

                else:
                    sql_query = None

            if sql_query is not None:
                try:
                    db_conn = self.__db_engine.connect()
                    db_conn.execute(sqlalchemy.text(sql_query))
                    db_conn.commit()
                    db_conn.close()

                except Exception as exc: # pylint: disable=broad-except
                    __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
                    err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
                    err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'

                    self.__err_logger.error(err_msg)
                    result = False

        msg_out = f'END TEST AND CREATE PARTITION TABLE, exchange: {self.__exchange}'
        msg_out += f', symbol: {symbol}, interval: {interval}, table_name: {table_name}'

        self.__log_logger.info(msg_out)

        return result

    def get_symbol_first_year_month_listed(self, symbol, interval):
        """
        get_symbol_first_year_month_listed
        ==================================
            This method return last value in column id
                :param self: This instance.
                :param symbol: str.
                :param interval: str.

                :return tuple: Return a tuple first element is first year listed\
                    and second element is first month listed

        """
        result = None
        cache_type = 'first_year_month_listed'

        __data = self.__get_data_from_cache_table(symbol, interval, cache_type)

        if __data is not None\
            and isinstance(__data, dict)\
            and 'year' in __data\
            and 'month' in __data:
            result = (int(__data['year']), int(__data['month']))
        else:
            __data = (
                self.__exchange_aux_class.get_symbol_first_year_month_listed(symbol,\
                                                                             interval,\
                                                                             self.__trading_type)
            )

            if __data is not None and isinstance(__data, tuple) and len(__data) >= 2:
                data_save = {}
                data_save['year'] = __data[0]
                data_save['month'] = __data[1]

                if self.__put_data_from_cache_table(symbol, interval, cache_type, data_save):
                    result = __data

        return result

    def __get_data_from_cache_table(self, symbol, interval, cache_type):
        result = None

        key_sha256_hash = self.__get_key_cache_table(symbol, interval, cache_type)
        stmt = sqlalchemy.select(sqlalchemy.column('data'))\
            .select_from(sqlalchemy.table(self.__cache_table_name))\
                .where(sqlalchemy.column('key_sha256_hash') == key_sha256_hash).limit(1)

        if stmt is not None:
            __data = None

            try:
                __data = self.__db_conn.execute(stmt)
                row = __data.fetchone()
                result_json_base64 = row[0] if row is not None else None
                __data.close()

                if isinstance(result_json_base64, str):
                    result_json = base64.b64decode(result_json_base64).decode('utf-8')

                    if ecf.is_json(result_json):
                        result = json.loads(result_json)

            except Exception as exc: # pylint: disable=broad-except
                __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
                err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)

                result = None

        return result

    def __put_data_from_cache_table(self, symbol, interval, cache_type, data):
        result = False

        key_sha256_hash = self.__get_key_cache_table(symbol, interval, cache_type)

        if self.__db_conn is not None:
            table = sqlalchemy.Table(self.__cache_table_name,\
                                     self.__db_metadata,\
                                     autoload_with=self.__db_conn)

            try:
                data_base64 = base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8')
                data_values = {}
                data_values['key_sha256_hash'] = key_sha256_hash
                data_values['data'] = data_base64

                __primary_key_cols = []
                __primary_key_cols.append('key_sha256_hash')

                if self.__db_type == 'postgresql':
                    stmt = sqlalchemy.dialects.postgresql.insert(table).values(data_values)
                    stmt = stmt.on_conflict_do_update(\
                        index_elements=__primary_key_cols, set_=data_values)
                elif self.__db_type == 'mysql':
                    stmt = sqlalchemy.dialects.mysql.insert(table).values(data_values)
                    stmt = stmt.on_duplicate_key_update(**data_values)

                self.__db_conn.execute(stmt)
                self.__db_conn.commit()

                result = True

            except Exception as exc: # pylint: disable=broad-except
                __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
                err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)

                result = False

        return result

    def __get_key_cache_table(self, symbol, interval, cache_type):
        result = None

        key_sha256_hash = f'_____{self.__exchange}_{symbol}_{interval}'
        key_sha256_hash = key_sha256_hash + f'_{self.__trading_type}_{cache_type}_____'
        result = str(hashlib.sha256(key_sha256_hash.encode('utf-8')).hexdigest())

        return result

    def __get_table_partition_name(self, symbol, interval, year):
        result = None
        table_name = self.__get_table_name(symbol, interval)
        result = table_name + '__' + str(year)
        return result

    def __check_if_exists_table_partition(self, symbol, interval, year):
        """
            For mysql databases only check if the last partition has been created.
        """
        result = False
        table_name = self.__get_table_name(symbol, interval)
        partition_name = self.__get_table_partition_name(symbol, interval, year)

        sql_query = None

        try:
            if self.__db_type == 'postgresql':
                sql_query = "SELECT COUNT(relname) FROM pg_class WHERE relname = '"\
                    + str(partition_name) + "'" + " AND relispartition = true ;"

            elif self.__db_type == 'mysql':
                sql_query = "SELECT COUNT(PARTITION_NAME) FROM information_schema.PARTITIONS "\
                    + "WHERE TABLE_SCHEMA = '" + str(self.__db_name)\
                    + "' AND TABLE_NAME = '" + str(table_name)\
                    + "' AND PARTITION_NAME = '" + str(partition_name) + "' ;"

            else:
                sql_query = None

            if sql_query is not None:
                db_conn = self.__db_engine.connect()
                __num_part = int(db_conn.execute(sqlalchemy.text(sql_query)).scalar())

                if __num_part > 0:
                    result = True

                db_conn.close()

        except Exception as exc: # pylint: disable=broad-except
            __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
            err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}, error: {exc}'
            err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
            self.__err_logger.error(err_msg)

            result = None

        return result

    def __exec_db_upsert_stmt(self, symbol, interval, data_list,\
                              db_conn, force_commit_after_stmt=False): # pylint: disable=too-many-arguments, too-many-locals
        result = True

        table_name = self.__get_table_name(symbol, interval)

        table = None
        __primary_key_cols = []
        __primary_key_cols.append(self.__primary_key_col)

        total_stmt = 0

        time_ini = time.time_ns()
        time_diff = 0
        min_time_diff = 0
        max_time_diff = 0

        commit_counter = 0
        commit_limit = 1000

        if db_conn is not None:
            table = sqlalchemy.Table(table_name, self.__db_metadata, autoload_with=db_conn)

            try:
                for data in data_list:
                    data_out_insert = data
                    data_out_update = data
                    data_out_insert['status'] = '__NON_CHECK__'
                    data_out_update.pop('status', None)

                    if self.__db_type == 'postgresql':
                        stmt = sqlalchemy.dialects.postgresql.insert(table).values(data_out_insert)
                        stmt = stmt.on_conflict_do_update(\
                            index_elements=__primary_key_cols, set_=data_out_update)
                    elif self.__db_type == 'mysql':
                        stmt = sqlalchemy.dialects.mysql.insert(table).values(data_out_insert)
                        stmt = stmt.on_duplicate_key_update(**data_out_update)

                    # pprint.pprint(data_out, sort_dicts=False)
                    # print('=' * 80)
                    # print()
                    # print(stmt)
                    # print('=' * 80)
                    # print()

                    db_conn.execute(stmt)
                    if force_commit_after_stmt:
                        db_conn.commit()
                    else:
                        if commit_counter >= commit_limit:
                            time.sleep(0.00005)
                            db_conn.commit()
                            commit_counter = 0
                        else:
                            commit_counter = commit_counter + 1

                    time_diff = time.time_ns() - time_ini
                    time_ini = time.time_ns()
                    min_time_diff = min(min_time_diff, time_diff)
                    max_time_diff = max(max_time_diff, time_diff)
                    time.sleep(0.00005)
                    total_stmt = total_stmt + 1

                result = True

                db_conn.commit()

            except Exception as exc: # pylint: disable=broad-except
                __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
                err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)

                result = False

        return result

    def check_fetch_data_struct(self, fecth_data):
        """
        check_fetch_data_struct
        =======================
            This method return true if fetch_data struct it is correct
                :param self: This instance.
                :param fetch_data: list[dict].

                :return: bool.
        """

        result = False

        if fecth_data is not None and isinstance(fecth_data, list) and len(fecth_data) > 0:
            result = True

            for data_n in fecth_data:
                result = result and isinstance(data_n, dict) and 'symbol' in data_n\
                    and 'interval' in data_n and isinstance(data_n['symbol'], str)\
                    and isinstance(data_n['interval'], str)\
                    and self.__exchange_aux_class.if_symbol_supported(data_n['symbol'])\
                    and data_n['interval'] in Ehdtd.get_supported_intervals(self.__exchange)

        return result

    def get_first_open_time_in_db(self, symbol, interval, db_conn=None):
        """
        get_first_open_time_in_db
        =========================
            This method return first value in column open_time
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param db_conn: database connection object.

                :return: int.
        """

        result = 0
        table_name = self.__get_table_name(symbol, interval)
        column_name = "open_time"

        if db_conn is None:
            db_conn = self.__db_conn

        if table_name is not None:
            stmt = None

            try:
                stmt = sqlalchemy.select(\
                    sqlalchemy.sql.functions.min(sqlalchemy.column(column_name))\
                     .label('last_time')).select_from(sqlalchemy.table(table_name))

            except Exception: # pylint: disable=broad-except
                stmt = None

            if stmt is not None:
                first_time = None
                try:
                    first_time = db_conn.execute(stmt)
                except Exception: # pylint: disable=broad-except
                    first_time = None

                if first_time is not None:
                    first_time = first_time.scalar()

                    if first_time is not None:
                        result = int(first_time)

        return result

    def get_last_open_time_in_db(self, symbol, interval, db_conn=None):
        """
        get_last_open_time_in_db
        ========================
            This method return last value in column open_time
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param db_conn: database connection object.

                :return: int.
        """

        result = 0
        table_name = self.__get_table_name(symbol, interval)
        column_name = "open_time"

        if db_conn is None:
            db_conn = self.__db_conn

        if table_name is not None:
            stmt = None

            try:
                stmt = sqlalchemy.select(\
                    sqlalchemy.sql.functions.max(sqlalchemy.column(column_name))\
                     .label('last_time')).select_from(sqlalchemy.table(table_name))

            except Exception: # pylint: disable=broad-except
                stmt = None

            if stmt is not None:
                last_time = None
                try:
                    last_time = db_conn.execute(stmt)
                except Exception: # pylint: disable=broad-except
                    last_time = None

                if last_time is not None:
                    last_time = last_time.scalar()

                    if last_time is not None:
                        result = int(last_time)

        return result

    def get_last_close_time_in_db(self, symbol, interval, db_conn=None):
        """
        get_last_close_time_in_db
        =========================
            This method return last value in column close_time
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param db_conn: database connection object.

                :return: int.
        """

        result = 0

        table_name = self.__get_table_name(symbol, interval)
        column_name = "close_time"

        if db_conn is None:
            db_conn = self.__db_conn

        if table_name is not None:
            stmt = None

            try:
                stmt = sqlalchemy.select(\
                    sqlalchemy.sql.functions.max(sqlalchemy.column(column_name))\
                     .label('last_time')).select_from(sqlalchemy.table(table_name))

            except Exception: # pylint: disable=broad-except
                stmt = None

            if stmt is not None:
                last_time = None
                try:
                    last_time = db_conn.execute(stmt)
                except Exception: # pylint: disable=broad-except
                    last_time = None

                if last_time is not None:
                    last_time = last_time.scalar()

                    if last_time is not None:
                        result = int(last_time)
        if result == 0:

            __year, __month = (
                self.get_symbol_first_year_month_listed(symbol,\
                                                        interval)
            )

            if __year is not None and __month is not None:
                result = int(round(datetime.datetime(__year,__month, 1, 0, 0, 0, 0).timestamp()))

        return result

    def get_min_unchecked_open_time_in_db(self, symbol, interval, db_conn=None):
        """
        get_min_unchecked_open_time_in_db
        =================================
            This method return min unchecked value in column open_time
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param db_conn: database connection object.

                :return: int.
        """

        result = 0
        table_name = self.__get_table_name(symbol, interval)
        column_name = "open_time"
        column_status = 'status'

        if db_conn is None:
            db_conn = self.__db_conn

        if table_name is not None:
            stmt = None

            try:
                stmt = sqlalchemy.select(\
                    sqlalchemy.sql.functions.min(sqlalchemy.column(column_name))\
                     .label('last_time')).select_from(sqlalchemy.table(table_name))\
                     .where(sqlalchemy.column(column_status) == '__NON_CHECK__')

            except Exception: # pylint: disable=broad-except
                stmt = None

            if stmt is not None:
                last_time = None
                try:
                    last_time = db_conn.execute(stmt)
                except Exception: # pylint: disable=broad-except
                    last_time = None

                if last_time is not None:
                    last_time = last_time.scalar()

                    if last_time is not None:
                        result = int(last_time)

        return result

    def get_min_valid_historical_open_time(self, symbol, interval, db_conn=None):
        """
        get_min_valid_historical_open_time
        ==================================
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param db_conn: database conection object.

                :return: int unix time of min open_time checked data.
        """

        result = 0
        table_name = self.__get_table_name(symbol, interval)
        column_open_time = 'open_time'
        column_status = 'status'

        if db_conn is None:
            db_conn = self.__db_conn

        stmt = None

        try:
            stmt_sub_max = sqlalchemy.select(\
                sqlalchemy.sql.functions.max(sqlalchemy.column(column_open_time))\
                .label('max_open_time')).select_from(sqlalchemy.table(table_name))\
                .where(sqlalchemy.column(column_status) != '__OK__').scalar_subquery()

            stmt_sub_min = sqlalchemy.select(\
                sqlalchemy.sql.functions.coalesce(\
                sqlalchemy.sql.functions.min(sqlalchemy.column(column_open_time)), 0)\
                .label('min_open_time')).select_from(sqlalchemy.table(table_name))\
                .scalar_subquery()

            stmt = sqlalchemy.select(sqlalchemy.column(column_open_time))\
                .select_from(sqlalchemy.table(table_name))\
                .where(sqlalchemy.column(column_open_time) ==\
                        sqlalchemy.sql.functions.coalesce(\
                            stmt_sub_max,\
                            stmt_sub_min))

        except Exception as exc: # pylint: disable=broad-except
            __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
            err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
            err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
            self.__err_logger.error(err_msg)

            stmt = None

        if stmt is not None:
            min_valid = None
            try:
                min_valid = db_conn.execute(stmt)
            except Exception: # pylint: disable=broad-except
                min_valid = None

            if min_valid is not None:
                min_valid = min_valid.scalar()

                if min_valid is not None:
                    result = int(min_valid)

        return result

    def __check_database_all_symbols_thd(self,\
                                         symbol: str=None,\
                                         interval: str=None,\
                                         start_from: int=0):
        result = False

        if symbol is None and interval is None:
            __to_exec = False

            __tables = [self.__get_table_name(node['symbol'], node['interval'])\
                        for node in self.__fetch_data ]

            __tables.append('__ALL__')

            to_run = all(
                self.__chk_db_all_symbols_thd[__table_name] is None\
                    or not self.__chk_db_all_symbols_thd[__table_name].is_alive()\
                        for __table_name in __tables
                )

            if to_run:

                __l_args = (start_from,)
                self.__chk_db_all_symbols_thd['__ALL__'] = (
                    threading.Thread(target=self.__check_database_all_symbols,\
                                    name='ehdtd_check_database_all_symbols',\
                                    args=__l_args)
                )

                self.__chk_db_all_symbols_thd['__ALL__'].start()
                result = True

        elif isinstance(symbol, str) and isinstance(interval, str):
            __table_name = self.__get_table_name(symbol, interval)

            with self.__lock_schedule:
                if self.__chk_db_all_symbols_thd[__table_name] is None\
                    or not self.__chk_db_all_symbols_thd[__table_name].is_alive():

                    __thread_name = f'ehdtd_check_database_{__table_name}'
                    __l_args = (symbol, interval, start_from)
                    self.__chk_db_all_symbols_thd[__table_name] = (
                        threading.Thread(target=self.check_database_data,\
                                        name=__thread_name,\
                                        args=__l_args)
                    )

                    self.__chk_db_all_symbols_thd[__table_name].start()
                    result = True

        return result

    def __check_database_all_symbols(self, start_from=0):
        result = False

        for fetch_data_node in self.__fetch_data:
            self.check_database_data(fetch_data_node['symbol'],\
                                     fetch_data_node['interval'],\
                                     start_from)

        return result

    def check_and_fix_database_data(self, last_n_values=None):
        """
        check_and_fix_database_data
        ===========================
        """
        result = False
        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access

        if not isinstance(self.__fetch_data, list):
            return result

        __symbol = None
        __interval = None

        try:
            log_msg = f'Starting {__l_function}, exchange: {self.__exchange}'
            self.__log_logger.info(log_msg)

            for __data in self.__fetch_data:
                __symbol = __data['symbol']
                __interval = __data['interval']
                __start_time = 0
                if last_n_values is not None:
                    __start_time = abs(int(time.time())\
                                    - (last_n_values\
                                        * Ehdtd.get_delta_seconds_for_interval(__interval)))

                log_msg = f'{__l_function} starting checking and fixing,'
                log_msg += f' exchange: {self.__exchange}'
                log_msg += f', symbol: {__symbol}, interval: {__interval}'
                log_msg += f', start_time: {__start_time}'
                self.__log_logger.info(log_msg)

                __db_errors = self.check_database_data(__symbol, __interval, __start_time)
                result = self.try_to_fix_database_data(__db_errors['result']) and result

                log_msg = f'{__l_function} ending checking and fixing, exchange: {self.__exchange}'
                log_msg += f', symbol: {__symbol}, interval: {__interval}'
                self.__log_logger.info(log_msg)

            log_msg = f'Ending {__l_function}, exchange: {self.__exchange}'
            self.__log_logger.info(log_msg)

        except Exception as exc: # pylint: disable=broad-except
            err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
            err_msg += f', symbol: {__symbol}, interval: {__interval}, error: {exc}'
            self.__err_logger.error(err_msg)
            time.sleep(5)
            result = False


        return result

    def check_database_data(self, symbol, interval, start_from=0):
        """
        check_database_data
        ===================
            This method return account of errors
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param start_from: int

                :return: dict
        """
        result = None
        batch_size = 7200

        db_conn = self.__db_engine.connect()

        table_name = self.__get_table_name(symbol, interval)
        __l_queue = self.__signals_queue_checks[table_name]
        last_open_time_in_db = self.get_last_open_time_in_db(symbol, interval, db_conn)

        result = {}
        result['result'] = []
        result['last_open_time_check'] = 0
        result['result_counter'] = 0
        result['time_proc_sec'] = 0

        stop_proc = False
        last_open_time_check = start_from
        offset = 0

        while last_open_time_check < last_open_time_in_db and not stop_proc:
            result_add = self.check_database_data_batch(symbol, interval, last_open_time_check,\
                                                        offset, batch_size, db_conn)

            if result_add is not None and isinstance(result_add, dict)\
                and 'result' in result_add and 'last_open_time_check' in result_add\
                and 'time_proc_sec' in result_add and 'result_counter_batch' in result_add:
                if isinstance(result_add['result'], list):
                    result['result'].extend(result_add['result'])
                    result['last_open_time_check'] = max(int(result_add['last_open_time_check']),\
                                                        result['last_open_time_check'])

                    result['result_counter'] = result['result_counter']\
                        + result_add['result_counter_batch']
                    result['time_proc_sec'] = result['time_proc_sec'] + result_add['time_proc_sec']

                    last_open_time_check = int(result['last_open_time_check'])

            if __l_queue.qsize() > 0:
                message = None
                try:
                    message = __l_queue.get(False, 9)
                except Exception: # pylint: disable=broad-except
                    message = None

                if message is not None and isinstance(message, str) and message == '__STOP__':
                    stop_proc = True

            time.sleep(0.25)

        result['time_proc_sec'] = round(result['time_proc_sec'], 3)

        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
        if result is not None and isinstance(result, dict) and 'result' in result:
            result['result'] = sorted({d['open_time']: d for d in result['result']}.values(),\
                                      key=lambda x: x['open_time'])

            str_out = f'{__l_function} Ended check database data OK: exchange: {self.__exchange}'
            str_out += f', symbol: {symbol}, interval: {interval}'
            self.__log_logger.info(str_out)

        else:
            str_out = f'{__l_function} Ended check database data ERROR: exchange: {self.__exchange}'
            str_out += f', symbol: {symbol}, interval: {interval}'
            self.__log_logger.info(str_out)

        if db_conn:
            db_conn.close()

        return result

    def check_database_data_batch(self, symbol, interval,\
                                  start_from=0, offset=0, limit=None, db_conn=None): # pylint: disable=too-many-arguments, too-many-locals, too-many-statements
        """
        check_database_data_batch
        =========================
            This method return account of errors
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param start_from: int.
                :param offset: int
                :param limit: int
                :param db_conn: sqlalchemy connection object

                :return: dict
        """
        result = None

        db_conn_local = db_conn

        if db_conn_local is None:
            db_conn_local = self.__db_engine.connect()

        table_name = self.__get_table_name(symbol, interval)
        column_open_time = 'open_time'
        column_open_date = 'open_date'
        column_close_time = 'close_time'
        column_close_date = 'close_date'
        column_volume = 'volume'
        column_status = 'status'

        last_open_time_in_db = self.get_last_open_time_in_db(symbol, interval, db_conn_local)
        time_ini = int(time.time_ns())

        table = sqlalchemy.Table(table_name, self.__db_metadata, autoload_with=db_conn_local)

        offset = offset - 5

        if offset < 0:
            offset = 0

        if limit <= 0:
            limit = 5

        limit_local = limit + 5

        commit_counter = 0
        commit_limit = 1000

        __delta_seconds = (
            Ehdtd.get_delta_seconds_for_interval(interval)
        )

        if interval == '1mo':
            __delta_seconds = Ehdtd.get_delta_seconds_for_interval('1d') * 31

        start_from = int(start_from) - (5 * __delta_seconds)
        if start_from < 0:
            start_from = 0

        if table_name is not None:
            stmt = None

            try:
                stmt = sqlalchemy.select(sqlalchemy.column(column_open_time),\
                                         sqlalchemy.column(column_close_time),\
                                         sqlalchemy.column(column_status),\
                                         sqlalchemy.column(column_open_date),\
                                         sqlalchemy.column(column_close_date),\
                                         sqlalchemy.column(column_volume))\
                                         .select_from(sqlalchemy.table(table_name))\
                                         .where(sqlalchemy.column(column_open_time) >= start_from)\
                                         .order_by(sqlalchemy.column(column_open_time).asc())\
                                         .offset(offset).limit(limit_local)


                results = db_conn_local.execute(stmt).fetchall()

                update_stmts = []
                result = {}
                result['result'] = []
                result['last_open_time_check'] = 0
                result['time_proc_sec'] = 0
                result['result_counter_batch'] = 0
                result['time_proc_sec'] = 0
                result_counter = 0

                last_index = len(results) - 1

                for i, fila in enumerate(results):

                    status = '__ERROR__'

                    if interval == '1mo':
                        __year, __month, __day = Ehdtd.get_ymd_from_time(int(fila[0]))
                        __delta_seconds = (
                            Ehdtd.get_delta_seconds_for_interval(interval, __year, __month)
                        )

                    if i >= 0:
                        if i == last_index:
                            if i == last_open_time_in_db:
                                status = '__NON_CHECK__'
                            else:
                                status = fila[2]

                        elif fila[1] == results[i+1][0]\
                            and __delta_seconds == (fila[1] - fila[0]):
                            status = '__OK__'
                        else:
                            status = '__ERROR__'

                    if status == '__ERROR__':
                        result_counter = result_counter + 1

                        data_col_err = {}
                        data_col_err['exchange'] = self.__exchange
                        data_col_err['symbol'] = symbol
                        data_col_err['interval'] = interval
                        data_col_err['open_time'] = fila[0]
                        data_col_err['open_date'] = fila[3]
                        data_col_err['close_time'] = fila[1]
                        data_col_err['close_date'] = fila[4]
                        data_col_err['status'] = status
                        data_col_err['diff_open_close'] = int(fila[1]) - int(fila[0])

                        if i == last_index:
                            data_col_err['diff_data'] = None
                        else:
                            data_col_err['diff_data'] = results[i+1][0] - fila[0]

                        data_col_err['open_date'] = (
                            time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(data_col_err['open_time']))
                        )
                        data_col_err['close_date'] = (
                            time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(data_col_err['close_time']))
                        )

                        result['result'].append(data_col_err)

                    if status != fila[2]:
                        update_dict = {}
                        update_dict[column_status] = status

                        update_stmt = None
                        update_stmt = sqlalchemy.update(table)\
                            .where(sqlalchemy.column(column_open_time) == fila[0])\
                            .values(update_dict)

                        update_stmts.append(update_stmt)

                    result['last_open_time_check'] = max(fila[0], result['last_open_time_check'])

                for stmt in update_stmts:
                    db_conn_local.execute(stmt)

                    if commit_counter >= commit_limit:
                        time.sleep(0.00005)
                        db_conn_local.commit()
                        commit_counter = 0
                    else:
                        commit_counter = commit_counter + 1

                    time.sleep(0.00005)

                db_conn_local.commit()

                time_diff = (int(time.time_ns()) - time_ini) / 1000000000

                result['result_counter_batch'] = result_counter
                result['time_proc_sec'] = round(time_diff,3)

            except Exception as exc: # pylint: disable=broad-except
                __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
                err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)
                time.sleep(5)

                result = None

        if db_conn is None and db_conn_local:
            db_conn_local.close()

        return result

    def try_to_fix_database_data(self, data):
        """
        try_to_fix_database_data
        =========================
            This method return account of errors
                :param self: This instance.
                :param data: list[dict]\
                    data = self.check_database_data(symbol, interval, start_from)

                :return: bool
        """

        result = False

        db_conn = self.__db_engine.connect()

        if data is not None and isinstance(data, list) and len(data) > 0:
            __limit = 1000
            __first_node = data[0]
            symbol = __first_node['symbol']
            interval = __first_node['interval']
            __delta_seconds = Ehdtd.get_delta_seconds_for_interval(interval)
            if interval == '1mo':
                __delta_seconds = Ehdtd.get_delta_seconds_for_interval('1d') * 31

            str_out = f'Begin Trying to fix database data: exchange: {self.__exchange}'
            str_out += f', symbol: {symbol}, interval: {interval}'
            self.__log_logger.info(str_out)

            for __error_node in data:
                __get_node_data_ot = int(__error_node['open_time']) - round((0.5 * __delta_seconds))
                __get_node_open_date = time.strftime("%Y-%m-%d %H:%M:%S",\
                                                     time.gmtime(__get_node_data_ot))

                str_out = f'Get data from exchange: {self.__exchange}, symbol: {symbol},'
                str_out += f' interval: {interval}, start_time: {__get_node_data_ot},'
                str_out += f' start_date: {__get_node_open_date}, limit: {__limit}'

                __get_data = (
                    self.__exchange_aux_class.get_last_klines_candlestick_data(symbol, interval,\
                                                                               start_time=\
                                                                               __get_node_data_ot,\
                                                                               limit=__limit)
                )

                if __get_data is not None and isinstance(__get_data, list) and len(__get_data) >= 2:
                    if 'open_time' in __get_data[0] and 'close_time' in __get_data[0]\
                        and 'open_time' in __get_data[1] and 'close_time' in __get_data[1]\
                        and 'volume' in __get_data[0]:
                        if interval == '1mo':
                            __year, __month, __day = (
                                self.get_ymd_from_time(int(__get_data[0]['open_time']))
                            )

                            __delta_seconds = (
                                Ehdtd.get_delta_seconds_for_interval('1mo',__year, __month)
                            )

                        if (int(__get_data[0]['close_time']) - int(__get_data[0]['open_time']))\
                            == __delta_seconds\
                            and int(__get_data[0]['close_time']) == int(__get_data[1]['open_time']):

                            if self.__exec_db_upsert_stmt(symbol, interval, __get_data, db_conn):

                                str_out += ' -> YES'

                                __next_open_time = None

                            else:
                                str_out += ' -> NO'
                    else:
                        str_out += ' -> NO'
                else:
                    str_out += ' -> NO'

                self.__log_logger.info(str_out)

            str_out = f'End Trying to fix database data: exchange: {self.__exchange}'
            str_out += f', symbol: {symbol}, interval: {interval}'
            self.__log_logger.info(str_out)

        if db_conn:
            db_conn.close()

        return result

    def __run_main_thread(self):
        result = True

        msg_out = f'BEGIN MAIN THREAD, exchange: {self.__exchange}'
        self.__log_logger.info(msg_out)

        __l_queue = self.__signals_queue_gets['__MAIN_THREAD__']

        __total_threads = len(self.__signals_queue_gets) + 1
        __running_threads = self.get_num_threads_active()

        __diff_threads_limit = 3

        self.__ccxw_class.start()

        time.sleep(45)

        msg_out = 'CCXW Sqlite temporal database size '
        msg_out += f'{self.__ccxw_class.get_sqlite_memory_used_human_readable()}'
        self.__log_logger.info(msg_out)

        __stop_run_main_thd = False
        message = None

        __sqlite_log_interval_time = 900

        __time_ini = time.time()
        __time_end = time.time()

        __schedule_task = None
        start_from = 0

        with self.__lock_schedule:
            __schedule_task = schedule.every(1)\
                .hours.at("14:23")\
                    .do(self.__check_database_all_symbols_thd, start_from=start_from)

        __last_check_time = int(time.time())
        __last_full_check_time = 0
        __time_to_check = 300
        __hour_to_full_check = 21

        while not __stop_run_main_thd:
            with self.__lock_schedule:
                schedule.run_pending()

            time.sleep(1)
            if __l_queue.qsize() > 0:
                try:
                    message = __l_queue.get(False, 9)
                except Exception: # pylint: disable=broad-except
                    message = None

                __time_end = time.time()
                if (__time_end - __time_ini) > __sqlite_log_interval_time:
                    msg_out = 'CCXW Sqlite temporal database size '
                    msg_out += (
                        f'{self.__ccxw_class.get_sqlite_memory_used_human_readable()}'
                    )
                    self.__log_logger.info(msg_out)
                    __time_ini = __time_end

                if message is not None and isinstance(message, str):
                    if message == '__STOP__':
                        __stop_run_main_thd = True

            __current_time = int(time.time())

            __start_time = 7200
            __current_hour = int(time.strftime("%H", time.gmtime(__current_time)))
            __current_minute = int(time.strftime("%M", time.gmtime(__current_time)))

            if __current_hour == __hour_to_full_check\
                and abs(__current_time - __last_full_check_time) > 7200:
                __start_time = None
                self.check_and_fix_database_data(__start_time)
                __last_check_time = int(time.time())
                __last_full_check_time = __last_check_time
            elif ((__current_minute % 5) == 0 and abs(__current_time - __last_check_time) > 90)\
                or abs(__current_time - __last_check_time) > __time_to_check:
                __start_time = 7200
                self.check_and_fix_database_data(__start_time)
                __last_check_time = int(time.time())

        with self.__lock_schedule:
            if __schedule_task is not None:
                schedule.cancel_job(__schedule_task)

            __tables = [self.__get_table_name(node['symbol'], node['interval'])\
                        for node in self.__fetch_data ]
            __tables.append('__ALL__')

            for __table in __tables:
                if self.__chk_db_all_symbols_thd[__table] is not None\
                    and self.__chk_db_all_symbols_thd[__table].is_alive():
                    self.__chk_db_all_symbols_thd[__table].join(300)

        msg_out = 'CCXW Sqlite temporal database size '
        msg_out += f'{self.__ccxw_class.get_sqlite_memory_used_human_readable()}'
        self.__log_logger.info(msg_out)

        self.__ccxw_class.stop()
        msg_out = f'END MAIN THREAD, exchange: {self.__exchange}'
        self.__log_logger.info(msg_out)

        return result

    def start(self):
        """
        start
        =====
            This method starting getting historic data from exchange and put into database.
                :param self: Ehdtd instance.
                :return bool: Return True if starting OK or False 
        """

        result = True

        self.__stop_running = False

        self.__threads = []

        for fetch_data_node in self.__fetch_data:
            __l_args = (fetch_data_node['symbol'], fetch_data_node['interval'])
            try:
                __thread_name = (
                    self.__get_table_name(fetch_data_node['symbol'], fetch_data_node['interval'])
                )
                __thread_name = 'ehdtd_' + __thread_name
                __data_thread = threading.Thread(target=self.__run_fetch_data_thread,\
                                                 args=__l_args, name=__thread_name)

                __data_thread.start()
                self.__threads.append(__data_thread)
                result = result and True
                time.sleep(0.25)
            except Exception as exc: # pylint: disable=broad-except
                __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
                err_msg = (
                    f'Found error in {__l_function}, exchange: {self.__exchange}, error: {exc}'
                )
                self.__err_logger.error(err_msg)

                result = False

        self.__main_thread = (
            threading.Thread(target=self.__run_main_thread, name='ehdtd_main_thread')
        )
        self.__main_thread.start()

        self.__is_threads_running = True

        if not result:
            msg_err = 'The treads not starting'
            print(msg_err)
            self.stop()

        return result

    def get_num_threads_active(self):
        """
            get_num_threads_active
            ======================
                :param self: Ehdtd instance.

                :return int: num of active threads

        """

        result = 0

        result = int(threading.active_count())

        return result

    def stop(self, time_to_wait=300):
        """
        stop
        ====
            This method stopping getting historic data from exchange and put into database.
                :param self: Ehdtd instance.
                :param time_to_wait: int Max time to wait the thread ended.

                :return bool: Return True if stopping OK or False 
        """
        result = False

        self.__stop_running = True

        for thread_queue_key, thread_queue in self.__signals_queue_gets.items():
            if thread_queue_key != '__MAIN_THREAD__':
                thread_queue.put('__STOP__')

        self.__signals_queue_gets['__MAIN_THREAD__'].put('__STOP__')

        for thread_queue_key, thread_queue in self.__signals_queue_checks.items():
            if thread_queue_key != '__MAIN_THREAD__':
                thread_queue.put('__STOP__')
                self.__signals_queue_checks[thread_queue_key].put('__STOP__')

        for __data_thread in self.__threads:
            if __data_thread.is_alive():
                __data_thread.join(time_to_wait)

        if self.__main_thread.is_alive():
            # print(f'main_thread_name: {self.__main_thread.getName()}')
            self.__main_thread.join(time_to_wait)

        self.__is_threads_running = False

        return result

    def get_websocket_kline_current_data(self, symbol, interval, last_n_values=9):
        """
        get_websocket_kline_current_data
        ================================
        """
        return self.__get_websocket_kline_current_data(symbol, interval, last_n_values)

    def __get_websocket_kline_current_data(self, symbol, interval, last_n_values=9):
        result = None

        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
        endpoint = 'kline'

        try:
            __current_data = self.__ccxw_class.get_current_data(endpoint, symbol, interval)

            if __current_data is not None\
                and isinstance(__current_data, dict)\
                and 'data' in __current_data\
                and __current_data['data'] is not None\
                and isinstance(__current_data['data'], list)\
                and len(__current_data['data']) > 0:

                __current_data = __current_data['data'][-last_n_values:]

                result = []

                for kline in __current_data:
                    if kline['is_closed'] is None or (kline['is_closed'] is True):
                        __kline_add = {}
                        __kline_add['open_time'] = int(round(int(kline['open_time'])/1000))
                        __kline_add['open_date'] = kline['open_time_date']
                        __kline_add['open_price'] = kline['open']
                        __kline_add['close_time'] = int(round(int(kline['close_time'])/1000))
                        __kline_add['close_date'] = kline['close_time_date']
                        __kline_add['close_price'] = kline['close']
                        __kline_add['low'] = kline['low']
                        __kline_add['high'] = kline['hight']
                        __kline_add['volume'] = kline['volume']
                        __kline_add['exchange'] = kline['exchange']
                        __kline_add['symbol'] = kline['symbol']
                        __kline_add['interval'] = kline['interval']

                        result.append(__kline_add)

        except Exception as exc: # pylint: disable=broad-except
            err_msg = f'Found error in {__l_function}, {self.__exchange}'
            err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
            self.__err_logger.error(err_msg)
            result = None

        return result

    def __run_fetch_data_thread(self, symbol, interval):
        result = False

        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
        __table_name = self.__get_table_name(symbol, interval)

        __l_queue = self.__signals_queue_gets[__table_name]

        msg_out = f'BEGIN THREAD: exchange: {self.__exchange}'
        msg_out += f', symbol: {symbol}, interval: {interval}'
        self.__log_logger.info(msg_out)

        __schedule_task_0 = None
        __schedule_task_1 = None
        __local_db_conn = None

        __table_name = self.__get_table_name(symbol, interval)

        if self.__db_engine is not None:
            __local_db_conn = self.__db_engine.connect()

        __stop_run = False

        __interval_sleep = 5
        __interval_seconds = Ehdtd.get_delta_seconds_for_interval(interval)

        if self.__init_db(symbol, interval, __local_db_conn)\
            and self.__init_db_with_kline(symbol, interval, __local_db_conn):

            message = None
            try:
                if __l_queue.qsize() > 0:
                    message = __l_queue.get(False, 9)

                    if message is not None and isinstance(message, str):
                        if message == '__STOP__':
                            __stop_run = True
            except Exception as exc: # pylint: disable=broad-except
                err_msg = f'Found error in {__l_function}, {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)

                message = None

            if not __stop_run:
                with self.__lock_schedule:
                    if interval.endswith('m'):
                        __task_every = 1

                        if not __stop_run:
                            __schedule_task_0 = schedule.every(__task_every)\
                                .minutes.at(":05")\
                                    .do(self.__get_and_proc_data_from_websocket, symbol, interval)
                            __schedule_task_1 = schedule.every(__task_every)\
                                .minutes.at(":40")\
                                    .do(self.__get_and_proc_data_from_websocket, symbol, interval)

                    elif interval.endswith('h'):
                        __task_every = 1
                        __schedule_task_0 = schedule.every(__task_every)\
                            .hours.at("00:50")\
                                .do(self.__get_and_proc_data_from_websocket, symbol, interval)
                        __schedule_task_1 = schedule.every(__task_every)\
                            .hours.at("05:23")\
                                .do(self.__get_and_proc_data_from_websocket, symbol, interval)

                    elif interval.endswith('d')\
                        or interval.endswith('w')\
                        or interval.endswith('mo'):
                        __task_every = 1
                        __schedule_task_0 = schedule.every(__task_every)\
                            .hours.at("09:23")\
                                .do(self.__get_and_proc_data_from_websocket, symbol, interval)

                        __schedule_task_1 = schedule.every(__task_every)\
                            .hours.at("39:23")\
                                .do(self.__get_and_proc_data_from_websocket, symbol, interval)

            time.sleep(__interval_sleep)

            start_from = 0
            self.__check_database_all_symbols_thd(symbol=symbol,\
                                                  interval=interval,\
                                                  start_from=start_from)

            while not __stop_run:

                message = None
                try:
                    if __l_queue.qsize() > 0:
                        message = __l_queue.get(False, 9)

                        if message is not None\
                            and isinstance(message, str)\
                            and message == '__STOP__':
                            __stop_run = True

                except Exception as exc: # pylint: disable=broad-except
                    err_msg = f'Found error in {__l_function}, {self.__exchange}'
                    err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                    self.__err_logger.error(err_msg)
                    message = None

                time.sleep(__interval_sleep)

            with self.__lock_schedule:
                if __schedule_task_0 is not None:
                    schedule.cancel_job(__schedule_task_0)
                time.sleep(1)

                if __schedule_task_1 is not None:
                    schedule.cancel_job(__schedule_task_1)

                time.sleep(1)

            time.sleep(1)

        if __local_db_conn is not None:
            __local_db_conn.close()

        msg_out = f'END THREAD: exchange: {self.__exchange}'
        msg_out += f', symbol: {symbol}, interval: {interval}'
        self.__log_logger.info(msg_out)

        return result

    def __get_and_proc_data_from_websocket(self, symbol, interval):
        result = True

        __exchanges_drop_1 = ['bingx', 'kucoin']

        __current_minute = int(time.strftime("%M", time.gmtime(int(round(time.time())))))

        __task_every = 1
        if interval.endswith('m'):
            __task_every = int(interval[:-1])

        if (__current_minute % __task_every) != 0:
            return result

        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access

        __local_db_conn = None
        if self.__db_engine is not None:
            __local_db_conn = self.__db_engine.connect()

        __last_n_values = 9
        if interval == '1m':
            __last_n_values = 27

        __current_data = self.__get_websocket_kline_current_data(symbol, interval, __last_n_values)

        str_out = f'{__l_function}: GET THIS DATA (From kline websocket api endpoint): '
        str_out += f'exchange: {self.__exchange}, symbol: {symbol}, interval: {interval}'

        if __current_data is not None\
            and isinstance(__current_data, list)\
            and len(__current_data) > 0:

            if self.__exchange in __exchanges_drop_1:
                __current_data = __current_data[0:-1]

            if self.__exec_db_upsert_stmt(symbol,\
                                          interval,\
                                          __current_data,\
                                          __local_db_conn):
                str_out += ' -> YES'
            else:
                str_out += ' -> NO'

        else:
            str_out += ' -> NO'

        self.__log_logger.info(str_out)

        if interval == '1mo':
            __last_time_in_db = (
                self.get_last_open_time_in_db(symbol, interval, __local_db_conn)
            )
            __year, __month, __day = (
                self.get_ymd_from_time(__last_time_in_db)
            )

            __interval_seconds = (
                Ehdtd.get_delta_seconds_for_interval(interval, int(__year), int(__month))
            )

        return result

    def __init_db_with_kline(self, symbol, interval, db_conn):
        result = True

        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
        __table_name = self.__get_table_name(symbol, interval)
        __l_queue = self.__signals_queue_gets[__table_name]

        __delta_seconds = Ehdtd.get_delta_seconds_for_interval(interval)
        __current_time = round(time.time())

        __last_time_in_db = self.get_last_close_time_in_db(symbol, interval, db_conn)

        try_counter_limit = 5
        try_counter = 0
        __stop_run = False

        while not __stop_run\
            and __last_time_in_db < (__current_time - ((2 * __delta_seconds) + 1)):

            __last_time_in_db = self.get_last_close_time_in_db(symbol, interval, db_conn)
            __start_time = __last_time_in_db - (10 * __delta_seconds)
            __year, __month, __day = self.get_ymd_from_time(__start_time)

            str_out = f'{__l_function}: GET THIS DATA (From kline api endpoint): '
            str_out += f'exchange: {self.__exchange}, symbol: {symbol}, interval: {interval}'
            str_out += f', year: {__year}, month: {__month}, day: {__day}'

            __hist_data = self.__exchange_aux_class.get_last_klines_candlestick_data(symbol,\
                                                                                     interval,\
                                                                                     __start_time)

            # pprint.pprint(__hist_data, sort_dicts=False)
            # print('=' * 80)
            # print('')

            if __hist_data is not None:
                if self.__exec_db_upsert_stmt(symbol, interval, __hist_data, db_conn):
                    str_out += ' -> YES'
                    try_counter = 0
                else:
                    str_out += ' -> NO'
                    try_counter += 1
            else:
                str_out += ' -> NO'
                try_counter += 1

            self.__log_logger.info(str_out)

            if try_counter > try_counter_limit:
                __stop_run = True

            message = None
            try:
                result = True
                if __l_queue.qsize() > 0:
                    message = __l_queue.get(False, 9)

                    if message is not None and isinstance(message, str) and message == '__STOP__':
                        __stop_run = True
                        result = False
            except Exception as exc: # pylint: disable=broad-except
                err_msg = f'Found error in {__l_function}, {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)
                result = False

                message = None

            time.sleep(14)
            __current_time = round(time.time())

        return result

    def __init_db(self, symbol, interval, db_conn):

        result = True
        if not self.__exchange_aux_class.has_historical_data_from_url_file():
            return result

        __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access

        __table_name = self.__get_table_name(symbol, interval)
        __l_queue = self.__signals_queue_gets[__table_name]

        try_counter_limit = 5
        try_counter = 0

        __daily_limit_seconds = 50400

        __stop_run = False
        force_daily = False

        __current_time = round(time.time())
        __current_year, __current_month, __current_day = (
            Ehdtd.get_ymd_from_time(__current_time)
        )

        __last_time_in_db = self.get_last_open_time_in_db(symbol, interval, db_conn)
        __last_year_in_db, __last_month_in_db = 1970, 1
        __start_year, __start_month = 1970, 1

        if __last_time_in_db == 0:
            __last_day_in_db = 1
            __last_year_in_db, __last_month_in_db = (
                self.get_symbol_first_year_month_listed(symbol, interval)
            )
            __start_year, __start_month = __last_year_in_db, __last_month_in_db
        else:
            __last_year_in_db, __last_month_in_db, __last_day_in_db = (
                Ehdtd.get_ymd_from_time(__last_time_in_db)
            )

            __start_year, __start_month = (
                Ehdtd.get_prev_year_month(__last_year_in_db, __last_month_in_db)
            )

        __stop_year, __stop_month = (
            Ehdtd.get_prev_year_month(__current_year, __current_month)
        )

        __stop_time = int(datetime.datetime(__stop_year, __stop_month, 28, 0, 0, 0,\
                                tzinfo=datetime.timezone.utc).timestamp())

        __year, __month = __start_year, __start_month
        __day = None

        while not __stop_run:

            str_out = f'{__l_function}: GET THIS DATA: exchange: {self.__exchange}'
            str_out += f', symbol: {symbol}, interval: {interval}'
            str_out += f', year: {__year}, month: {__month}'

            __hist_data = None

            with self.__lock_thread_get_file:

                __hist_data = (
                    self.__exchange_aux_class.get_historical_data_from_url_file(symbol,\
                                                                                interval,\
                                                                                __year,\
                                                                                __month,\
                                                                                __day,\
                                                                                force_daily)
                )

            if __hist_data is not None\
                and self.__exec_db_upsert_stmt(symbol, interval, __hist_data, db_conn):
                str_out += ' -> YES'
                try_counter = 0
            else:
                str_out += ' -> NO'
                try_counter += 1

            self.__log_logger.info(str_out)

            if try_counter == 0 or try_counter > try_counter_limit:
                __year, __month = Ehdtd.get_next_year_month(__year, __month)
                try_counter = 0


            __time = int(datetime.datetime(__year, __month, 28, 0, 0, 0,\
                                    tzinfo=datetime.timezone.utc).timestamp())

            if __time > __stop_time:
                __stop_run = True

            message = None
            try:
                result = True
                if __l_queue.qsize() > 0:
                    message = __l_queue.get(False, 9)

                    if message is not None and isinstance(message, str):
                        if message == '__STOP__':
                            __stop_run = True
                            result = False

            except Exception as exc: # pylint: disable=broad-except
                err_msg = f'Found error in {__l_function}, {self.__exchange}'
                err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
                self.__err_logger.error(err_msg)
                result = False

            time.sleep(1)

        return result

    def get_data_from_db(self,\
                         symbol,\
                         interval,\
                         start_from: int=0,\
                         until_to: int=None,\
                         return_type: str='pandas'):
        """
        get_data_from_db
        ================
            This method return data from db
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param start_from: int
                :param until_to: int
                :param return_type: str 'pandas', 'list', 'list_consistent_streams'\
                    or 'list_consistent_streams_pandas'

                :return: Pandas dataframe, list[dict], list[list[dict]], list[list[dataframe]]
                :rtype: Pandas dataframe, list[dict], list[list[dict]], list[dataframe]

        """
        result = None

        if symbol is None\
            or interval is None\
            or not isinstance(symbol, str)\
            or not isinstance(interval, str)\
            or interval not in Ehdtd.get_supported_intervals(self.__exchange):
            return result
        if start_from is None or not isinstance(start_from, int):
            return result

        if until_to is None:
            until_to = int(round(time.time()))
        elif not isinstance(until_to, int):
            return result

        if return_type is None or return_type not in\
            ['pandas', 'list', 'list_consistent_streams', 'list_consistent_streams_pandas']:
            return_type = 'pandas'

        db_conn_local = self.__db_engine.connect()
        table_name = self.__get_table_name(symbol, interval)

        column_open_time = 'open_time'
        column_close_time = 'close_time'
        column_open = 'open_price'
        column_close = 'close_price'
        column_low = 'low'
        column_high = 'high'
        column_volume = 'volume'
        column_status = 'status'

        table = sqlalchemy.Table(table_name, self.__db_metadata, autoload_with=db_conn_local)

        try:
            stmt = sqlalchemy.select(\
                                        sqlalchemy.column(column_open_time),\
                                        sqlalchemy.column(column_close_time),\
                                        sqlalchemy.cast(sqlalchemy.column(column_open),\
                                                        sqlalchemy.Float).label(column_open),\
                                        sqlalchemy.cast(sqlalchemy.column(column_close),\
                                                        sqlalchemy.Float).label(column_close),\
                                        sqlalchemy.cast(sqlalchemy.column(column_low),\
                                                        sqlalchemy.Float).label(column_low),\
                                        sqlalchemy.cast(sqlalchemy.column(column_high),\
                                                        sqlalchemy.Float).label(column_high),\
                                        sqlalchemy.cast(sqlalchemy.column(column_volume),\
                                                        sqlalchemy.Float).label(column_volume),\
                                        sqlalchemy.column(column_status))\
                                        .select_from(table)\
                                        .where(sqlalchemy.and_(\
                                            sqlalchemy.column(column_open_time) >= start_from),\
                                            sqlalchemy.column(column_open_time) <= until_to)\
                                        .order_by(sqlalchemy.column(column_open_time).asc())

            results = db_conn_local.execute(stmt).fetchall()

            result = []

            consistent_data_num = None

            if results is not None and isinstance(results, list):
                file_status_prev = None

                for row in results:
                    file_add = None
                    file_add = {}
                    file_add[column_open_time] = row[0]
                    file_add[column_close_time] = row[1]
                    file_add[column_open] = row[2]
                    file_add[column_close] = row[3]
                    file_add[column_low] = row[4]
                    file_add[column_high] = row[5]
                    file_add[column_volume] = row[6]
                    file_add[column_status] = row[7]

                    if return_type in ['list', 'pandas']:
                        result.append(file_add)

                    elif return_type\
                        in ['list_consistent_streams', 'list_consistent_streams_pandas']:

                        if file_add[column_status] == '__OK__':
                            if file_status_prev is None\
                                or file_status_prev != file_add[column_status]:
                                result.append([])

                                if consistent_data_num is None:
                                    consistent_data_num = 0
                                else:
                                    consistent_data_num += 1

                            result[consistent_data_num].append(file_add)

                    file_status_prev = file_add[column_status]

                if return_type == 'pandas':
                    result = pandas.DataFrame(result)
                elif return_type == 'list_consistent_streams_pandas':
                    for i, data in enumerate(result):
                        result[i] = pandas.DataFrame(data)

        except Exception as exc: # pylint: disable=broad-except
            __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
            err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
            err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
            self.__err_logger.error(err_msg)
            time.sleep(5)

            result = None

        if db_conn_local:
            db_conn_local.close()

        return result

    def __init_loggin_out(self, log_dir: str=None):
        result = False

        if self.__set_default_log_file(log_dir=log_dir):
            try:
                if not self.__debug:
                    for handler in logging.root.handlers[:]:
                        logging.root.removeHandler(handler)

                __log_formatter = (
                    logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S -')
                )

                __err_formatter = (
                    logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S -')
                )

                log_handler = logging.FileHandler(self.__log_logger_file)
                log_handler_w = logging.handlers.WatchedFileHandler(self.__log_logger_file)
                log_handler.setFormatter(__log_formatter)
                self.__log_logger = logging.getLogger(f'EHDTD_{self.__exchange.upper()}_LOG')
                self.__log_logger.setLevel(logging.INFO)

                for handler in self.__log_logger.handlers[:]:
                    self.__log_logger.removeHandler(handler)

                self.__log_logger.addHandler(log_handler)
                self.__log_logger.addHandler(log_handler_w)

                err_handler = logging.FileHandler(self.__err_logger_file)
                err_handler_w = logging.handlers.WatchedFileHandler(self.__err_logger_file)
                err_handler.setFormatter(__err_formatter)
                self.__err_logger = logging.getLogger(f'EHDTD_{self.__exchange.upper()}_ERR')
                self.__err_logger.setLevel(logging.ERROR)

                for handler in self.__err_logger.handlers[:]:
                    self.__err_logger.removeHandler(handler)

                self.__err_logger.addHandler(err_handler)
                self.__err_logger.addHandler(err_handler_w)
                result = True

            except Exception: # pylint: disable=broad-except
                result = False

        return result

    def __set_default_log_file(self, log_dir: str=None):
        """
        Ehdtd __set_default_log_file function.
        ==============================================
            This method .
                :param self: Ehdtd instance.
                :param log_dir: str.

                :return: str log_file
        """
        __result = False
        __log_file_path = None
        __log_file_base = 'ehdtd'
        __log_file_log = None
        __log_file_err = None

        if log_dir is not None and self.is_valid_dir(log_dir):
            __log_file_log = os.path.join(log_dir, f'{__log_file_base}-{self.__exchange}.log')
            __log_file_err = os.path.join(log_dir, f'{__log_file_base}-{self.__exchange}.err')
            if self.is_valid_file(__log_file_log) and self.is_valid_file(__log_file_err):
                __result = True

        if not __result:
            current_directory = os.getcwd()
            is_poetry_active = os.path.exists(os.path.join(current_directory, 'poetry.lock'))

            if is_poetry_active:
                __log_file_path = os.path.join(current_directory, '.log')
            else:
                __home_directory = os.path.expanduser("~")
                __log_file_path = os.path.join(__home_directory, '.var/log/ehdtd')

            if __log_file_path is not None and self.is_valid_dir(__log_file_path):
                __log_file_log = os.path.join(__log_file_path,\
                                              f'{__log_file_base}-{self.__exchange}.log')

                __log_file_err = os.path.join(__log_file_path,\
                                              f'{__log_file_base}-{self.__exchange}.err')

                if self.is_valid_file(__log_file_log) and self.is_valid_file(__log_file_err):
                    __result = True

        if __result:
            self.__log_logger_file = __log_file_log
            self.__err_logger_file = __log_file_err

        return __result

    def is_valid_dir(self, dir_in): # pylint: disable=unused-argument
        """
        is_valid_dir
        ============
            This function get a string and check if is a valid directory and write access
                :param self: Ehdtd instance.
                :param dir_in: str.

                :return bool:
        """

        result = False

        try:
            if not os.path.exists(dir_in):
                os.makedirs(dir_in)
                result = True
            elif not os.path.isdir(dir_in):
                result = False
            else:
                result = True

            result = result and os.access(dir_in, os.W_OK)

        except Exception: # pylint: disable=broad-except
            result = False

        return result

    def is_valid_file(self, file_in): # pylint: disable=unused-argument
        """
        is_valid_file
        =============
            This function get a string and check if is a valid file and write access
                :param self: Ehdtd instance.
                :param file_in: str.

                :return bool:
        """

        result = False

        try:
            if not os.path.exists(file_in):
                with open(file_in, 'a', encoding="utf-8") as f:
                    f.write("")
                result = True
            elif not os.path.isfile(file_in):
                result = False
            else:
                result = True

            result = result and os.access(file_in, os.W_OK)

        except Exception: # pylint: disable=broad-except
            result = False

        return result

    @classmethod
    def get_last_klines_candlestick_data(cls, exchange, symbol, interval,\
                                         start_time=None, limit=1000): # pylint: disable=too-many-arguments
        """
        get_last_klines_candlestick_data function.
        ==========================================
            This method return a kline data from exchange.
                :param cls: Ehdtd Class.
                :param exchange: str
                :param symbol: str
                :param interval: str
                :param start_time: int unix timestamp if is None start_time is time.time() - 900
                :param limit: int if limit is greater than 1000, 1000 is asigned

                :return: list[dict].
        """
        result = None

        if not (exchange is not None and isinstance(exchange, str)\
                and exchange in Ehdtd.get_supported_exchanges()):
            raise ValueError('The exchange is invalid')

        __aux_class = EhdtdExchangeConfig.exchange_classes[exchange]()

        if not (symbol is not None and isinstance(symbol, str)\
                and __aux_class.if_symbol_supported(symbol)):
            raise ValueError('Symbol is invalid')

        if not (interval is not None and isinstance(interval, str)\
                and interval in Ehdtd.get_supported_intervals(exchange)):
            raise ValueError('Interval is invalid')

        if not (start_time is not None and isinstance(start_time, int)\
                and start_time <= time.time()):
            start_time = round(time.time_ns() / 1000000000) - 900

        if not (limit is not None and isinstance(limit, int) and limit > 0):
            limit = 1000

        result = __aux_class.get_last_klines_candlestick_data(symbol, interval, start_time, limit)

        return result

    @classmethod
    def get_supported_exchanges(cls):
        """
        get_supported_exchanges.
        ========================
            This method return a list of supported exchanges.
                :param cls: Ehdtd Class.

                :return: list of supported exchanges.
        """
        __suported_exchanges = ['binance', 'bybit', 'okx', 'kucoin', 'bingx', 'binanceus']

        return __suported_exchanges

    @classmethod
    def get_exchange_connectivity(cls, exchange='binance'):
        """
        get_exchange_connectivity.
        =========================
            This function return a dict with connectivity information.
                :param cls: Ehdtd Class.
                :param exchange: str.

                :return dict: result.
                    result = {
                        'result': bool, # True if connectivity is working False in other case.
                        'code': int | None, # Error Code
                        'msg': str | None # Error message
                    }

        """
        __result = None

        __result = (
            EhdtdExchangeConfig.exchange_classes[exchange].get_exchange_connectivity()
        )

        return __result

    @classmethod
    def get_supported_trading_types(cls):
        """
        get_supported_trading_types.
        ============================
            This method return a list of supported trading_types.
                :param cls: Ehdtd Class.

                :return: list of supported trading_types.
        """
        __suported_trading_types = ['SPOT']

        return __suported_trading_types

    @classmethod
    def get_supported_intervals(cls, exchange='binance'):
        """
        get_supported_intervals.
        ========================
            This method return a list of supported intervals.
                :param cls: Ehdtd Class.
                :param exchange: str.

                :return: list of supported intervals.
        """
        __result = None

        __exchange_intervals = (
            EhdtdExchangeConfig.exchange_classes[exchange].get_supported_intervals()
        )

        __ehdtd_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',\
                             '6h', '8h', '12h', '1d', '3d', '1w', '1mo']

        __result = []

        for interval in __exchange_intervals:
            if interval in __exchange_intervals:
                __result.append(interval)

        return __result

    @classmethod
    def not_daily_data(cls, exchange='binance'):
        """
        not_daily_data.
        ===============
            This method return a list of supported intervals.
                :param cls: Ehdtd Class.
                :param exchange: str.

                :return: list of not daily data for intervals.
        """
        __result = None

        __result = EhdtdExchangeConfig.exchange_classes[exchange].not_daily_data()

        return __result

    @classmethod
    def get_supported_databases(cls):
        """
        get_supported_databases.
        ========================
            This method return a list of supported databases.
                :param cls: Ehdtd Class.

                :return: list of supported databases.
        """
        __suported_databases = ['postgresql', 'mysql']

        return __suported_databases

    @classmethod
    def get_delta_seconds_for_interval(cls, interval='1m', year=None, month=None):
        """
        get_delta_seconds_for_interval.
        ===============================
            This method return a seconds for interval.
                :param cls: Ehdtd Class.
                :param interval: str.
                :param year: int.
                :param month: int.

                :return: int
        """
        __result = 60

        num_part = ''
        unit_part = ''

        for char in interval:
            if char.isdigit():
                num_part += char
            else:
                unit_part += char

        num_part = int(num_part)

        if unit_part == 's':
            __result = num_part
        elif unit_part == 'm':
            __result = num_part * 60
        elif unit_part == 'h':
            __result = num_part * 3600
        elif unit_part == 'd':
            __result = num_part * 86400
        elif unit_part == 'w':
            __result = num_part * 604800
        elif unit_part == 'mo':
            __last_day = 28
            if year is not None and month is not None:
                __result = 0
                for __i in range(0,num_part):
                    __last_day = int(calendar.monthrange(year, month)[1])
                    __result = __result + (86400 * __last_day)
                    year, month = Ehdtd.get_next_year_month(year, month)
            else:
                __result = num_part * 86400 * __last_day
        else:
            __result = num_part

        return __result

    @classmethod
    def seconds_from_midnight(cls, year, month, day):
        """
        seconds_from_midnight.
        ======================
            This method return a seconds from 0:00:00 hour to current time.
                :param cls: Ehdtd Class.
                :param year: int.
                :param month: int.
                :param day: int.

                :return: int
        """
        result = 0

        midnight_date = datetime.datetime(year, month, day, 0, 0, 0)
        current_time = datetime.datetime.now()
        diff = current_time - midnight_date
        result = int(diff.total_seconds())

        limit = 24 * 3600
        result = min(result, limit)

        return result

    @classmethod
    def get_ymd_from_time(cls, input_time=None):
        """
        get_ymd_from_time.
        ==================
            This method return a tuple with (year, month, day) for input_time.
                :param cls: Ehdtd Class.
                :param input_time: int.

                :return: tuple(int, int, int) (year, month, day)
        """
        result = None

        if input_time is None:
            input_time = round(time.time())

        __year = int(time.strftime("%Y",time.gmtime(input_time)))
        __month = int(time.strftime("%m",time.gmtime(input_time)))
        __day = int(time.strftime("%d",time.gmtime(input_time)))

        result = (__year, __month, __day)

        return result

    @classmethod
    def get_prev_year_month_day(cls, current_year, current_month, current_day=None):
        """
        get_prev_year_month_day.
        ========================
            This method return a tuple with previous (current_year, current_month, current_day).
                :param cls: Ehdtd Class.
                :param current_year: int.
                :param current_month: int.
                :param current_day: int.

                :return: tuple(int, int, int) (prev_year, prev_month, prev_day)
        """
        result = None

        __this_time = round(time.time())
        __this_year, __this_month, __this_day = Ehdtd.get_ymd_from_time(__this_time)

        __prev_day = current_day
        __prev_month = current_month
        __prev_year = current_year

        if current_day is None:
            __prev_day = current_day
            __prev_month = current_month - 1
            __prev_year = current_year

            if __prev_month <= 0:
                __prev_month = 12
                __prev_year = current_year - 1
        else:
            __prev_day = current_day - 1

            if __prev_day <= 0:
                __prev_month = current_month - 1
                __prev_year = current_year

                if __prev_month <= 0:
                    __prev_month = 12
                    __prev_year = current_year - 1

                __prev_day = calendar.monthrange(__prev_year, __prev_month)[1]

            else:
                __prev_month = current_month
                __prev_year = current_year

        result = (__prev_year, __prev_month, __prev_day)

        return result

    @classmethod
    def get_prev_year_month(cls, current_year, current_month):
        """
        get_prev_year_month.
        ====================
            This method return a tuple with previous (current_year, current_month).
                :param cls: Ehdtd Class.
                :param current_year: int.
                :param current_month: int.

                :return: tuple(int, int) (prev_year, prev_month)
        """
        result = None

        __prev_month = current_month - 1
        __prev_year = current_year

        if __prev_month <= 0:
            __prev_month = 12
            __prev_year = current_year - 1

        result = (__prev_year, __prev_month)

        return result

    @classmethod
    def get_next_year_month_day(cls, current_year, current_month, current_day=None):
        """
        get_next_year_month_day.
        ========================
            This method return a tuple with next (current_year, current_month, current_day).
                :param cls: Ehdtd Class.
                :param current_year: int.
                :param current_month: int.
                :param current_day: int.

                :return: tuple(int, int, int) (next_year, next_month, next_day)
        """
        result = None

        __this_time = round(time.time())
        __this_year, __this_month, __this_day = Ehdtd.get_ymd_from_time(__this_time)

        __next_year = current_year
        __next_month = current_month
        __next_day = current_day

        __last_day = calendar.monthrange(__next_year, __next_month)[1]

        if current_day is None:

            if current_month >= 12:
                __next_year = current_year + 1
                __next_month = 1
            else:
                __next_month = current_month + 1

            if current_year == __this_year and current_month == __this_month:
                result = (current_year, current_month, current_day)
            else:
                result = (__next_year, __next_month, current_day)

        else:
            if  current_day >= __last_day:
                __next_day = 1

                if current_month >= 12:
                    __next_year = current_year + 1
                    __next_month = 1
                else:
                    __next_month = current_month + 1
            else:
                __next_day = current_day + 1

            if current_year == __this_year\
                and current_month == __this_month\
                and current_day == __this_day:
                result = (current_year, current_month, current_day)
            else:
                result = (__next_year, __next_month, __next_day)


        return result

    @classmethod
    def get_next_year_month(cls, current_year, current_month):
        """
        get_next_year_month_day.
        ========================
            This method return a tuple with next (current_year, current_month).
                :param cls: Ehdtd Class.
                :param current_year: int.
                :param current_month: int.

                :return: tuple(int, int) (next_year, next_month)
        """
        result = None

        __next_year = current_year
        __next_month = current_month + 1

        if __next_month > 12:
            __next_year = current_year + 1
            __next_month = 1

        result = (__next_year, __next_month)

        return result

class EhdtdRO():
    """
    EhdtdRO - cryptoCurrency Exchange history data read only version.
    =================================================================
    This class retrieves historical data database.

    Example:
    ```python
    import time
    from ehdtd import EhdtdRO

    exchange = 'binance'
    symbol = 'BTC/USDT'
    interval = '1m'
    limit = 10

    db_data = {
        'db_type': 'postgresql',  # postgresql, mysql
        'db_name': 'ehdtd',
        'db_user': 'ehdtd',
        'db_pass': 'xxxxxxxxx',
        'db_host': '127.0.0.1',
        'db_port': '5432'
    }

    ehd_ro = EhdtdRO(exchange, db_data)  # Create an instance

    ```
    """

    def __init__(self,\
                 exchange,\
                 db_data: dict,\
                 trading_type: str='SPOT',\
                 debug: bool=False):
        """
        EhdtdRO constructor
        =================
            :param self: EhdtdRO instance.
            :param exchange: str exchange name.

            :param db_data: dict
                                dict must have dist struct
                                    {
                                        db_type: str, #Only supported 'postgresql' and 'mysql'.
                                        db_name: str, #Database name.
                                        db_user: str, #Database user.
                                        db_pass: str, #Database pass.
                                        db_host: str, #Database host.
                                        db_port: str  #Database port.
                                    }

            :param trading_type: str only allowed 'SPOT'
            :param debug: bool

            :return: Return a new instance of the Class EhdtdRO.
        """

        self.__db_type = None
        self.__db_name = None
        self.__db_engine = None
        self.__db_conn = None
        self.__exchange = None
        self.__trading_type = None
        self.__debug = debug

        if self.__debug is None or not isinstance(self.__debug, bool):
            err_msg = 'debug must be boolean True or False'
            raise ValueError(err_msg)

        if exchange is not None\
            and isinstance(exchange, str)\
            and len(exchange) > 0\
            and exchange in Ehdtd.get_supported_exchanges():
            self.__exchange = exchange
        else:
            err_msg = f'Wrong exchange name {exchange} or invalid exchange.'
            raise ValueError(err_msg)

        if trading_type is not None\
            and isinstance(trading_type, str)\
            and len(trading_type) > 0\
            and trading_type in Ehdtd.get_supported_trading_types():
            self.__trading_type = trading_type
        else:
            err_msg = f'Wrong trading type name {trading_type} or invalid trading type'
            raise ValueError(err_msg)

        if isinstance(db_data, dict) and all(key in db_data for key in\
            ['db_type', 'db_name', 'db_user', 'db_pass', 'db_host', 'db_port']):

            if all(isinstance(db_data[key], str) for key in\
                   ['db_type', 'db_name', 'db_user', 'db_pass', 'db_host', 'db_port']):
                self.__db_type = db_data['db_type']
                self.__db_name = db_data['db_name']

                if db_data['db_type'] in Ehdtd.get_supported_databases():
                    self.__db_type = db_data['db_type']
                else:
                    raise ValueError('The database type '\
                                     + str(db_data['db_type'])\
                                     + ' is not supported.')

                if self.__db_type == 'postgresql':
                    self.__db_uri = db_data['db_type'] + '://' + db_data['db_user'] + ':'\
                        + db_data['db_pass'] + '@' + db_data['db_host'] + ':'\
                        + db_data['db_port'] + '/' + self.__db_name
                elif self.__db_type == 'mysql':
                    self.__db_uri = db_data['db_type'] + '+pymysql://'\
                        + db_data['db_user'] + ':'\
                        + db_data['db_pass'] + '@' + db_data['db_host'] + ':'\
                        + db_data['db_port'] + '/' + self.__db_name

                if self.__check_database_connection():
                    self.__db_engine = sqlalchemy.create_engine(self.__db_uri)

                    self.__db_conn = self.__db_engine.connect()
                    self.__db_metadata = sqlalchemy.MetaData()
                    self.__db_metadata.reflect(bind=self.__db_conn)

                    try:
                        self.__db_engine = sqlalchemy.create_engine(self.__db_uri)

                        self.__db_conn = self.__db_engine.connect()
                        self.__db_metadata = sqlalchemy.MetaData()
                        self.__db_metadata.reflect(bind=self.__db_conn)

                    except Exception as exc: # pylint: disable=broad-except
                        err_msg = f'Error on connection to database {exc}'
                        print(err_msg)
                else:
                    err_msg = 'Error on connection to database'
                    raise ValueError(err_msg)
            else:
                err_msg = 'Wrong database data'
                raise ValueError(err_msg)

        else:
            err_msg = 'Wrong database data'
            raise ValueError(err_msg)

    def __check_table_exists(self, table_name):
        result = False

        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=self.__db_engine)

        try:
            table = metadata.tables[table_name] # pylint: disable=unused-variable
            result = True
        except Exception: # pylint: disable=broad-except
            result = False

        return result

    def check_if_exists_symbol_interval(self, symbol, interval):
        """
        check_if_exists_symbol_interval
        ===============================
        Check the connection to a database.

        Parameters:
            :param self: EhdtdRO instance.
            :param symbol: str.
            :param interval: str.

        Returns:
            :return bool: True if the symbol interval exists in db

        """

        result = False

        __table_name = self.__get_table_name(symbol, interval)

        if self.__check_table_exists(__table_name):
            result = True

        return result

    def __check_database_connection(self):
        """
        check_database_connection
        =========================

        Check the connection to a database.

        Parameters:
            :param self: EhdtdRO instance.

        Returns:
            :return bool: True if the connection is successful, False otherwise.
        """
        result = False

        try:
            engine = sqlalchemy.create_engine(self.__db_uri)
            with engine.connect() as connection: # pylint: disable=unused-variable
                result = True
        except Exception: # pylint: disable=broad-except
            result = False

        return result

    def __get_table_name(self, symbol, interval):
        result = f"{self.__exchange}__"
        result += f"{self.__trading_type.replace('/','_').replace('-','_').lower()}"
        result += f"__{symbol.replace('/','_').lower()}__{interval}"
        return result

    def get_data_from_db(self,\
                        symbol,\
                        interval,\
                        start_from: int=0,\
                        until_to: int=None,\
                        return_type: str='pandas'):
        """
        get_data_from_db
        ================
            This method return data from db
                :param self: This instance.
                :param symbol: str.
                :param interval: str.
                :param start_from: int
                :param until_to: int
                :param return_type: str 'pandas', 'list', 'list_consistent_streams'\
                    or 'list_consistent_streams_pandas'

                :return: Pandas dataframe, list[dict], list[list[dict]], list[list[dataframe]]
                :rtype: Pandas dataframe, list[dict], list[list[dict]], list[dataframe]

        """
        result = None

        if symbol is None\
            or interval is None\
            or not isinstance(symbol, str)\
            or not isinstance(interval, str)\
            or interval not in Ehdtd.get_supported_intervals(self.__exchange):
            return result
        if start_from is None or not isinstance(start_from, int):
            return result

        if until_to is None:
            until_to = int(round(time.time()))
        elif not isinstance(until_to, int):
            return result

        if return_type is None or return_type not in\
            ['pandas', 'list', 'list_consistent_streams', 'list_consistent_streams_pandas']:
            return_type = 'pandas'

        db_conn_local = self.__db_engine.connect()
        table_name = self.__get_table_name(symbol, interval)

        column_open_time = 'open_time'
        column_close_time = 'close_time'
        column_open = 'open_price'
        column_close = 'close_price'
        column_low = 'low'
        column_high = 'high'
        column_volume = 'volume'
        column_status = 'status'

        table = sqlalchemy.Table(table_name, self.__db_metadata, autoload_with=db_conn_local)

        try:
            stmt = sqlalchemy.select(\
                                        sqlalchemy.column(column_open_time),\
                                        sqlalchemy.column(column_close_time),\
                                        sqlalchemy.cast(sqlalchemy.column(column_open),\
                                                        sqlalchemy.Float).label(column_open),\
                                        sqlalchemy.cast(sqlalchemy.column(column_close),\
                                                        sqlalchemy.Float).label(column_close),\
                                        sqlalchemy.cast(sqlalchemy.column(column_low),\
                                                        sqlalchemy.Float).label(column_low),\
                                        sqlalchemy.cast(sqlalchemy.column(column_high),\
                                                        sqlalchemy.Float).label(column_high),\
                                        sqlalchemy.cast(sqlalchemy.column(column_volume),\
                                                        sqlalchemy.Float).label(column_volume),\
                                        sqlalchemy.column(column_status))\
                                        .select_from(table)\
                                        .where(sqlalchemy.and_(\
                                            sqlalchemy.column(column_open_time) >= start_from),\
                                            sqlalchemy.column(column_open_time) <= until_to)\
                                        .order_by(sqlalchemy.column(column_open_time).asc())

            results = db_conn_local.execute(stmt).fetchall()

            result = []

            consistent_data_num = None

            if results is not None and isinstance(results, list):
                file_status_prev = None

                for row in results:
                    file_add = None
                    file_add = {}
                    file_add[column_open_time] = row[0]
                    file_add[column_close_time] = row[1]
                    file_add[column_open] = row[2]
                    file_add[column_close] = row[3]
                    file_add[column_low] = row[4]
                    file_add[column_high] = row[5]
                    file_add[column_volume] = row[6]
                    file_add[column_status] = row[7]

                    if return_type in ['list', 'pandas']:
                        result.append(file_add)

                    elif return_type\
                        in ['list_consistent_streams', 'list_consistent_streams_pandas']:

                        if file_add[column_status] == '__OK__':
                            if file_status_prev is None\
                                or file_status_prev != file_add[column_status]:
                                result.append([])

                                if consistent_data_num is None:
                                    consistent_data_num = 0
                                else:
                                    consistent_data_num += 1

                            result[consistent_data_num].append(file_add)

                    file_status_prev = file_add[column_status]

                if return_type == 'pandas':
                    result = pandas.DataFrame(result)
                elif return_type == 'list_consistent_streams_pandas':
                    for i, data in enumerate(result):
                        result[i] = pandas.DataFrame(data)

        except Exception as exc: # pylint: disable=broad-except
            __l_function = sys._getframe().f_code.co_name # pylint: disable=protected-access
            err_msg = f'Found error in {__l_function}, exchange: {self.__exchange}'
            err_msg += f', symbol: {symbol}, interval: {interval}, error: {exc}'
            # self.__err_logger.error(err_msg)
            time.sleep(5)

            result = None

        if db_conn_local:
            db_conn_local.close()

        return result
    