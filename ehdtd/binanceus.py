"""
Ehdtd - cryptoCurrency Exchange history data to database
Binanceus auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import json
import time
import datetime
import urllib.request
import calendar

import ehdtd.ehdtd_common_functions as ecf

class BinanceusEhdtdAuxClass():
    """
    Ehdtd - cryptoCurrency Exchange history data to database BinanceusEhdtdAuxClass
    ===============================================================================
        This class contains helper functions for the Ehdtd class.

    """

    def __init__(self):
        """
        BinanceusEhdtdAuxClass constructor
        ==================================
            Initializes the BinanceusEhdtdAuxClass with the provided parameters.

                :param self: BinanceusEhdtdAuxClass instance.
                :return: Return a new instance of the Class BinanceusEhdtdAuxClass.
        """

        self.__exchange_info_cache = {}
        self.__exchange_info_cache['data'] = None
        self.__exchange_info_cache['last_get_time'] = 0

    def get_exchange_info(self):
        """
        get_exchange_info
        =================
            This function get exchange info. 
                :param self: Instance of this class
                :return dict: Return exchange info.
        """

        result = None
        max_last_get_time = 7200
        current_time = int(time.time())

        if (current_time - self.__exchange_info_cache['last_get_time']) >= max_last_get_time:
            self.__exchange_info_cache['data'] = None

        if self.__exchange_info_cache['data'] is None:

            __l_url_api = BinanceusEhdtdAuxClass.get_api_url()
            __l_endpoint = '/exchangeInfo'
            __l_url_point = __l_url_api + __l_endpoint
            __data = ecf.file_get_contents_url(__l_url_point)

            if __data is not None and ecf.is_json(__data):
                result = json.loads(__data)
                self.__exchange_info_cache['data'] = result
                self.__exchange_info_cache['last_get_time'] = current_time

        else:
            result = self.__exchange_info_cache['data']

        return result

    def get_exchange_full_list_symbols(self, sort_list=True):
        """
        get_exchange_full_list_symbols
        ==============================
            :param self: Instance of this class
            :param sort_list: bool.
            :return list: Return full list of symbols supported.
        """

        result = None
        __main_data = self.get_exchange_info()

        if __main_data is not None and isinstance(__main_data,dict)\
            and 'symbols' in __main_data and isinstance(__main_data['symbols'],list):
            result = []
            for symbol_data in __main_data['symbols']:
                if symbol_data is not None and isinstance(symbol_data,dict):
                    if 'baseAsset' in symbol_data and 'quoteAsset' in symbol_data\
                        and isinstance(symbol_data['baseAsset'],str)\
                        and isinstance(symbol_data['quoteAsset'],str):
                        result.append(\
                            symbol_data['baseAsset'].upper() + '/'\
                                + symbol_data['quoteAsset'].upper())

            if sort_list:
                result.sort()

        return result

    def if_symbol_supported(self, symbol):
        """
        if_symbol_supported
        ===================
            This function check if symbol is supported by the exchange.
                :param self: Instance of this class
                :param symbol: str.

                :return bool: Return True if supported 
        """
        result = False

        __data = self.get_exchange_full_list_symbols()

        if isinstance(__data, list) and symbol in __data:
            result = True

        return result

    def has_historical_data_from_url_file(self):
        """
        has_historical_data_from_url_file
        =================================
        """
        return False

    # pylint: disable=unused-argument
    def get_historical_data_from_url_file(self, symbol, interval, year, month, day=None,\
                                          force_daily=False, trading_type='SPOT'):
        """
        get_historical_data_from_url_file
        =================================
        """
        result = None
        return result

    def get_last_klines_candlestick_data(self, symbol, interval, start_time=None, limit=1000):
        """
        Ehdtd get_last_klines_candlestick_data function.
        ================================================
            This method return a list of last klines data.
                :param self: BinanceusEhdtdAuxClass instance.
                :param symbol: str
                :param interval: str
                :param start_time: int unix timestamp if is None start_time is time.time() - 900
                :param limit: int if limit is greater than 1000, 1000 is asigned

                :return: list[dict] of klines
        
        """
        result = None

        result = BinanceusEhdtdAuxClass.get_kline_data(symbol,\
                                                     interval,\
                                                     start_time=start_time,\
                                                     limit=limit)

        return result

    @classmethod
    def get_api_url(cls, trading_type: str='SPOT'):
        """
        get_api_url
        ===========
            This function set and return API URL.
                :param cls: BinanceusEhdtdAuxClass Class.
                :param trading_type: str only allowed 'SPOT'
                :return str: Return API URL.
        """
        result = None

        __url_api = None
        if trading_type == 'SPOT':
            __url_api = 'https://api.binance.us/api/v3'

        result = __url_api

        return result

    @classmethod
    def get_exchange_connectivity(cls):
        """
        get_exchange_connectivity
        =========================
            This function return a dict with connectivity information.
                :param cls: BinanceusEhdtdAuxClass Class.
                :return dict: result.
                    result = {
                        'result': bool, # True if connectivity is working False in other case.
                        'code': int | None, # Error Code
                        'msg': str | None # Error message
                    }
        """
        result = None
        headers = {}
        timeout = 45

        __url_api = cls.get_api_url()
        __endpoint = '/ping'
        __url = f'{__url_api}{__endpoint}'

        req = urllib.request.Request(__url, None, headers)
        if req is not None:
            try:
                with urllib.request.urlopen(req, None, timeout=timeout) as response:
                    result = {}
                    result['result'] = None
                    result['code'] = response.status
                    result['data'] = response.read()
                    result['headers'] = response.headers
                    result['headers_str'] = response.headers.as_string()
                    result['final_url'] = response.url
                    result['res_code'] = None
                    result['res_msg'] = None

            except Exception as exc: # pylint: disable=broad-except
                result = {}
                result['result'] = None
                result['code'] = None
                result['data'] = None
                result['headers'] = None
                result['headers_str'] = None
                result['final_url'] = None
                result['res_code'] = None
                result['res_msg'] = None

                if hasattr(exc, 'code'):
                    result['code'] = exc.code
                if hasattr(exc, 'read'):
                    result['data'] = exc.read().decode('utf-8')
                if hasattr(exc, 'headers'):
                    result['headers'] = exc.headers
                    header_str = ''
                    for header in exc.headers:
                        header_str += f'{header}: {exc.headers[header]}'
                    result['headers_str'] = header_str
                if hasattr(exc, 'url'):
                    result['final_url'] = exc.url

            if result is not None\
                and isinstance(result, dict)\
                and 'data' in result:

                if isinstance(result['data'], bytes):
                    result['data'] = result['data'].decode()

                if ecf.is_json(result['data']):
                    result['data'] = json.loads(result['data'])

                    if result['data'] is not None and isinstance(result['data'], dict):
                        if 'code' in result['data']:
                            result['res_code'] = result['data']['code']
                        if 'msg' in result['data']:
                            result['res_msg'] = result['data']['msg']
                            if isinstance(result['res_msg'], bytes):
                                result['res_msg'] = result['res_msg'].decode()

                if result['code'] is not None\
                    and isinstance(result['code'], int)\
                    and 200 <= result['code'] < 300\
                    and result['res_code'] is None:
                    result['result'] = True
                    result['headers'] = None
                    result['data'] = {}
                else:
                    result['result'] = False

        return result

    @classmethod
    def get_unified_symbol_from_symbol(cls, symbol):
        """
        get_unified_symbol_from_symbol
        ==============================
            This function get unified symbol from symbol.
                :param cls: BinanceusEhdtdAuxClass Class.
                :param symbol: str.
                :return str: Return unified symbol.
        """
        result = symbol
        beac = BinanceusEhdtdAuxClass()

        full_list_symbols = beac.get_exchange_full_list_symbols(False)

        if full_list_symbols is not None and isinstance(full_list_symbols, list):
            for symbol_rpl in full_list_symbols:
                if symbol_rpl is not None\
                    and symbol.replace('/', '').lower() == symbol_rpl.replace('/', '').lower():
                    result = symbol_rpl
                    break

        return result

    @classmethod
    def get_symbol_from_unified_symbol(cls, symbol):
        """
        get_symbol_from_unified_symbol
        ==============================
            This function get unified interval from interval.
                :param cls: BinanceusEhdtdAuxClass Class.
                :param symbol: str unified symbol.
                :return str: Return symbol.
        """
        result = symbol

        beac = BinanceusEhdtdAuxClass()

        if symbol is not None\
            and isinstance(symbol, str)\
            and len(symbol) > 0\
            and beac.if_symbol_supported(symbol):
            result = symbol.replace('/', '').upper()
            result = str(result)

        return result

    @classmethod
    def get_delta_time_from_interval(cls, interval, year=None, month=None):
        """
        get_delta_time_from_interval
        ============================
        """
        result = 60

        last_day = 30

        try:
            if year is not None and month is not None:
                last_day = calendar.monthrange(year, month)[1]

        except Exception: # pylint: disable=broad-except
            last_day = 30

        __delta_month = 86400 * last_day
        __intervals_map = {
            '1m': 60,
            '3m': 180,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '6h': 21600,
            '12h': 43200,
            '1d': 86400,
            '1w': 604800,
            '1mo': __delta_month
        }

        if interval in __intervals_map:
            result = __intervals_map[interval]

        return result

    @classmethod
    def get_kline_data(cls,\
                       symbol,\
                       interval,\
                       start_time: float=None,\
                       end_time: float=None,\
                       limit: int=1000,\
                       default_endpoint: str='uiKlines',\
                       trading_type: str='SPOT'):
        """
        get_kline_data
        ==============
            This function get the kline/candlestick API URL.
                :param cls: BinanceusEhdtdAuxClass Class.
                :param symbol: str
                :param interval: str
                :param start_time: float
                :param end_time: float
                :param limit: int
                :param default_endpoint: str only allowed 'klines' or 'uiKlines' default uiKlines
                :param trading_type: str only allowed 'SPOT'
                :return list[dict]:
        """

        result = None
        unified_symbol = symbol
        symbol = BinanceusEhdtdAuxClass.get_symbol_from_unified_symbol(symbol)
        limit = min(limit, 1000)

        if default_endpoint not in ['klines', 'uiKlines']:
            default_endpoint = 'uiKlines'

        url_base = BinanceusEhdtdAuxClass.get_api_url(trading_type=trading_type)
        url = f'{url_base}/{default_endpoint}?symbol={symbol}&interval={interval}'
        url += f'&limit={int(limit) + 1}'

        start_time_out = ''
        end_time_out = ''

        if start_time is not None and (isinstance(start_time, (int,float)) or start_time == 0):
            start_time = round(start_time * 1000)
            start_time_out = f'&startTime={start_time}'

        if end_time is not None and (isinstance(end_time, (int,float)) or end_time == 0):
            end_time = round(end_time * 1000)
            end_time_out = f'&endTime={end_time}'

        url = url + start_time_out + end_time_out

        __attemp = 0
        __max_attemp = 9
        req_data = ecf.file_get_contents_url_cmpl(url, mode='r')

        while __attemp < __max_attemp and not (req_data is not None and isinstance(req_data, dict)):
            req_data = ecf.file_get_contents_url_cmpl(url, mode='r')
            time.sleep(0.1)
            __attemp += 1

        if req_data is not None and isinstance(req_data, dict):

            if not req_data['exception_status']\
                and req_data['code'] == 200\
                and ecf.is_json(req_data['data']):

                data = json.loads(req_data['data'])

                if data is not None and isinstance(data, list):
                    result = []

                    for kline in data:
                        if kline is not None and isinstance(kline, list) and len(kline) >= 12:
                            data_line = None
                            data_line = {}
                            data_line['open_time'] = int(round(int(kline[0])/1000))
                            data_line['open_date'] = time.strftime("%Y-%m-%d %H:%M:%S",\
                                                                   time.gmtime(\
                                                                       data_line['open_time']))
                            data_line['open_price'] = kline[1]
                            data_line['close_time'] = int(round(int(kline[6])/1000))
                            data_line['close_date'] = time.strftime("%Y-%m-%d %H:%M:%S",\
                                                                    time.gmtime(\
                                                                        data_line['close_time']))
                            data_line['close_price'] = kline[4]
                            data_line['low'] = kline[3]
                            data_line['high'] = kline[2]
                            data_line['volume'] = kline[5]
                            data_line['exchange'] = 'binanceus'
                            data_line['symbol'] = unified_symbol
                            data_line['interval'] = interval
                            result.append(data_line)

                    if len(result) > 0:
                        result = result[:-1]

        return result

    @classmethod
    def get_next_month_time_from_time(cls, time_ini):
        """
        get_next_month_time_from_time
        =============================
        """
        result = 0
        delta_time = 3600 * 24 * 32

        next_time = time_ini + delta_time
        __next_year = int(time.strftime("%Y", time.gmtime(next_time)))
        __next_month = int(time.strftime("%m", time.gmtime(next_time)))

        result = int(round(datetime.datetime(__next_year,__next_month, 1, 0, 0, 0, 0).timestamp()))

        return result

    @classmethod
    def get_symbol_first_year_month_listed(cls, symbol, interval, trading_type: str='SPOT'):
        """
        get_symbol_first_year_month_listed
        ==================================
            This function set and return API URL.
                :param cls: BinanceusEhdtdAuxClass Class.
                :param symbol: str
                :param interval: str
                :param trading_type: str only allowed 'SPOT'
                :return tuple: Return a tuple first element is first year listed\
                               and second element is first month listed
        """
        __min_history_year = 2018
        __min_history_month = 1

        result = (__min_history_year, __min_history_month)

        data = BinanceusEhdtdAuxClass.get_kline_data(symbol,\
                                                   interval,\
                                                   start_time=0,\
                                                   limit=4,\
                                                   trading_type=trading_type)

        if data is not None\
            and isinstance(data, list)\
            and len(data) > 0\
            and data[0] is not None\
            and isinstance(data[0], dict):

            if 'open_time' in data[0]\
                and data[0]['open_time'] is not None\
                and isinstance(data[0]['open_time'], (int, float)):

                open_time = int(data[0]['open_time'])

                result = (int(time.strftime("%Y", time.gmtime(open_time))),\
                          int(time.strftime("%m", time.gmtime(open_time))))

        return result

    @classmethod
    def not_daily_data(cls):
        """
        Ehdtd get_supported_intervals function.
        =======================================
            This method return a list of not daily data for this intervals.
                :param cls: BinanceusEhdtdAuxClass Class.

                :return: list of not daily data intervals
        """
        __result = ['3d', '1w', '1mo']

        return __result

    @classmethod
    def get_supported_intervals(cls):
        """
        Ehdtd get_supported_intervals function.
        =======================================
            This method return a list of supported intervals.
                :param cls: BinanceusEhdtdAuxClass Class.

                :return: list of supported intervals.
        """
        __result = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',\
                    '6h', '8h', '12h', '1d', '3d', '1w', '1mo']

        return __result
