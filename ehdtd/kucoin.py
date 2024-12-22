"""
Ehdtd - cryptoCurrency Exchange history data to database
Kucoin auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import json
import time
import datetime
import urllib.request
import pprint # pylint: disable=unused-import
import calendar

import ehdtd.ehdtd_common_functions as ecf

class KucoinEhdtdAuxClass():
    """
    Ehdtd - cryptoCurrency Exchange history data to database KucoinEhdtdAuxClass
    =============================================================================
        This class contains helper functions for the Ehdtd class.

    """

    def __init__(self):
        """
        KucoinEhdtdAuxClass constructor
        ================================
            Initializes the KucoinEhdtdAuxClass with the provided parameters.

                :param self: KucoinEhdtdAuxClass instance.
                :return: Return a new instance of the Class KucoinEhdtdAuxClass.
        """

        self.__exchange_info_cache = {}
        self.__exchange_info_cache['data'] = None
        self.__exchange_info_cache['last_get_time'] = 0

    def get_exchange_info(self, trading_type='SPOT'):
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

            __l_url_api = KucoinEhdtdAuxClass.get_api_url()
            __l_endpoint = '/api/v2/symbols'
            __l_url_point = f'{__l_url_api}{__l_endpoint}?'
            __l_url_point += f'category={trading_type.lower()}&limit=1000'

            post_data = None
            headers = None

            __data = ecf.file_get_contents_url(__l_url_point, 'r', post_data, headers)

            if __data is not None and ecf.is_json(__data):
                __res = json.loads(__data)
                if __res is not None and isinstance(__res, dict)\
                    and 'code' in __res and 'data' in __res\
                    and int(__res['code']) == 200000:
                    self.__exchange_info_cache['data'] = __res['data']
                    self.__exchange_info_cache['last_get_time'] = current_time
                    result = __res['data']
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

        if __main_data is not None and isinstance(__main_data, list):
            try:
                result = []
                for symbol_data in __main_data:
                    if symbol_data is not None and isinstance(symbol_data, dict):
                        if 'baseCurrency' in symbol_data and 'quoteCurrency' in symbol_data\
                            and isinstance(symbol_data['baseCurrency'], str)\
                            and isinstance(symbol_data['quoteCurrency'], str):
                            result.append(\
                                symbol_data['baseCurrency'].upper() + '/'\
                                    + symbol_data['quoteCurrency'].upper())

                if sort_list:
                    result.sort()

            except Exception as e: # pylint: disable=broad-except
                print(f'ERROR: {e}')
                result = None

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

            :param self: KucoinEhdtdAuxClass instance.
            :param symbol: str unified symbol.
            :param interval: str.
            :param year: int. >=2018
            :param month: int. >=1 or <=12
            :param day: int. >=1 or <=31
            :param force_daily: bool force getting monthly and daily files.
            :param trading_type: str only allowed 'SPOT'.
            :return list[dict]: Return list of dict with data
        """
        result = None

        return result

    def get_last_klines_candlestick_data(self, symbol, interval, start_time=None, limit=1000):
        """
        Ehdtd get_last_klines_candlestick_data function.
        ================================================
            This method return a list of last klines data.
                :param self: KucoinEhdtdAuxClass instance.
                :param symbol: str
                :param interval: str
                :param start_time: int unix timestamp if is None start_time is time.time() - 900
                :param limit: int if limit is greater than 1000, 1000 is asigned

                :return: list[dict] of klines
        
        """
        result = None

        result = KucoinEhdtdAuxClass.get_kline_data(symbol,\
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
                :param cls: KucoinEhdtdAuxClass Class.
                :param trading_type: str only allowed 'SPOT'
                :return str: Return API URL.
        """
        result = None

        __url_api = None
        if trading_type == 'SPOT':
            __url_api = 'https://api.kucoin.com'

        result = __url_api

        return result

    @classmethod
    def get_exchange_connectivity(cls):
        """
        get_exchange_connectivity
        =========================
            This function return a dict with connectivity information.
                :param cls: KucoinEhdtdAuxClass Class.
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
        __endpoint = '/api/v1/status'
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
                        result['res_msg'] = None

                if result['data']['code'] is not None\
                    and isinstance(result['data']['code'], (int, str))\
                    and int(result['data']['code']) == 200000\
                    and 'data' in result['data']\
                    and result['data']['data'] is not None\
                    and isinstance(result['data']['data'], dict):

                    __tmp = result['data']['data']

                    if __tmp is not None and isinstance(__tmp, dict)\
                        and 'status' in __tmp and __tmp['status'] is not None\
                        and isinstance(__tmp['status'], str)\
                        and __tmp['msg'] is not None and isinstance(__tmp['msg'], str):

                        if __tmp['status'] == 'open':
                            result['result'] = True
                            result['headers'] = None
                            result['res_code'] = None
                            result['res_msg'] = None
                            result['data'] = {}
                        else:
                            result['result'] = False
                            result['res_msg'] = __tmp['msg']
                else:
                    result['result'] = False

        return result

    @classmethod
    def get_unified_symbol_from_symbol(cls, symbol):
        """
        get_unified_symbol_from_symbol
        ==============================
            This function get unified symbol from symbol.
                :param cls: KucoinEhdtdAuxClass Class.
                :param symbol: str.
                :return str: Return unified symbol.
        """
        result = symbol
        beac = KucoinEhdtdAuxClass()

        full_list_symbols = beac.get_exchange_full_list_symbols(False)

        if full_list_symbols is not None and isinstance(full_list_symbols, list):
            for symbol_rpl in full_list_symbols:
                if symbol_rpl is not None\
                    and symbol.replace('-', '').lower() == symbol_rpl.replace('/', '').lower():
                    result = symbol_rpl
                    break

        return result

    @classmethod
    def get_symbol_from_unified_symbol(cls, symbol):
        """
        get_symbol_from_unified_symbol
        ==============================
            This function get unified interval from interval.
                :param cls: KucoinEhdtdAuxClass Class.
                :param symbol: str unified symbol.
                :return str: Return symbol.
        """
        result = symbol

        beac = KucoinEhdtdAuxClass()

        if symbol is not None\
            and isinstance(symbol, str)\
            and len(symbol) > 0\
            and beac.if_symbol_supported(symbol):
            result = symbol.replace('/', '-').upper()
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
                :param cls: KucoinEhdtdAuxClass Class.
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
        symbol = KucoinEhdtdAuxClass.get_symbol_from_unified_symbol(symbol)

        url_base = KucoinEhdtdAuxClass.get_api_url(trading_type=trading_type)
        default_endpoint = ''
        if trading_type == 'SPOT':
            default_endpoint = '/api/v1/market/candles'

        req_interval = KucoinEhdtdAuxClass.get_interval_from_unified_interval(interval)

        limit = min(limit, 1000)

        url = f'{url_base}{default_endpoint}'
        url += f'?symbol={symbol}&type={req_interval}'

        start_time_out = ''
        end_time_out = ''

        if start_time is not None and isinstance(start_time, (int,float)) and start_time >= 0:
            start_time = int(round(start_time))
            start_time_out = f'&startAt={start_time}'

        if end_time is not None\
            and isinstance(end_time, (int,float))\
            and end_time >= 0\
            and end_time > start_time:
            end_time = int(round(end_time))
            end_time_out = f'&endAt={end_time}'

        __delta_time = cls.get_delta_time_from_interval(interval)
        if len(start_time_out) > 0:
            __time_limit = int(start_time + (limit * __delta_time))
            if len(end_time_out) == 0:
                end_time_out = f'&endAt={__time_limit}'
            else:
                if end_time > __time_limit:
                    end_time_out = f'&endAt={__time_limit}'
        else:
            if len(end_time_out) > 0:
                __time_limit = int(end_time - (limit * __delta_time))
                start_time_out = f'&startAt={__time_limit}'

        url += f'{start_time_out}{end_time_out}'

        __attemp = -1
        __max_attemp = 9
        req_data = None
        post_data = None
        headers = None

        while __attemp < __max_attemp and not (req_data is not None and isinstance(req_data, dict)):
            req_data = ecf.file_get_contents_url(url, 'r', post_data, headers)
            if ecf.is_json(req_data):
                req_data = json.loads(req_data)

            __attemp += 1
            time.sleep(0.1)

        if req_data is not None and isinstance(req_data, dict)\
            and 'code' in req_data and 'data' in req_data\
            and req_data['code'] is not None and int(req_data['code']) == 200000:

            try:
                __data = req_data['data']

                if __data is not None and isinstance(__data, list):
                    result = []
                    __delta_time = cls.get_delta_time_from_interval(interval)

                    for kline in __data:
                        if kline is not None and isinstance(kline, list) and len(kline) >= 7:
                            data_line = None
                            data_line = {}
                            data_line['open_time'] = int(round(int(kline[0])))
                            data_line['open_date'] = time.strftime("%Y-%m-%d %H:%M:%S",\
                                                                   time.gmtime(\
                                                                       data_line['open_time']))

                            if interval == '1mo':
                                year = int(time.strftime("%Y", time.gmtime(data_line['open_time'])))
                                month = (
                                    int(time.strftime("%m", time.gmtime(data_line['open_time'])))
                                )

                                __delta_time = (
                                    cls.get_delta_time_from_interval(interval, year, month)
                                )

                            __close_time = data_line['open_time'] + __delta_time
                            data_line['open_price'] = kline[1]
                            data_line['close_time'] = int(__close_time)
                            data_line['close_date'] = time.strftime("%Y-%m-%d %H:%M:%S",\
                                                                    time.gmtime(\
                                                                        data_line['close_time']))
                            data_line['close_price'] = kline[2]
                            data_line['low'] = kline[4]
                            data_line['high'] = kline[3]
                            data_line['volume'] = kline[5]
                            data_line['exchange'] = 'kucoin'
                            data_line['symbol'] = unified_symbol
                            data_line['interval'] = interval
                            result.append(data_line)

                    if len(result) > 1:
                        result.reverse()
                        # result = result[:-1]

            except Exception: # pylint: disable=broad-except
                result = None

        return result

    @classmethod
    def get_interval_from_unified_interval(cls, interval):
        """
        get_interval_from_unified_interval
        ==================================

        """
        result = None

        __intervals_map = {
            '1m': '1min',
            '3m': '3min',
            '5m': '5min',
            '15m': '15min',
            '30m': '30min',
            '1h': '1hour',
            '2h': '2hour',
            '4h': '4hour',
            '6h': '6hour',
            '8h': '8hour',
            '12h': '12hour',
            '1d': '1day',
            '1w': '1week',
            '1mo': '1month'
        }

        if interval in __intervals_map:
            result = str(__intervals_map[interval])

        return result

    @classmethod
    def get_next_month_time_from_time(cls, time_ini):
        """
        get_next_month_time_from_time
        =============================
        """
        result = 0
        delta_time = 3600 * 24 * 40

        __current_year = int(time.strftime("%Y", time.gmtime(time_ini)))
        __current_month = int(time.strftime("%m", time.gmtime(time_ini)))

        __current_time = (
            int(round(datetime.datetime(__current_year,__current_month, 1, 0, 0, 0, 0)\
                      .timestamp()))
        )

        next_time = __current_time + delta_time
        __next_year = int(time.strftime("%Y", time.gmtime(next_time)))
        __next_month = int(time.strftime("%m", time.gmtime(next_time)))

        result = int(round(datetime.datetime(__next_year,__next_month, 1, 0, 0, 0, 0).timestamp()))

        return result

    @classmethod
    def get_symbol_first_year_month_listed(cls, symbol, interval, trading_type: str='SPOT'): # pylint: disable=unused-argument
        """
        get_symbol_first_year_month_listed
        ==================================
            This function set and return API URL.
                :param cls: KucoinEhdtdAuxClass Class.
                :param symbol: str
                :param interval: str
                :param trading_type: str only allowed 'SPOT'
                :return tuple: Return a tuple first element is first year listed\
                               and second element is first month listed
        """
        result = None

        __min_history_year = 2018
        __min_history_month = 1

        unified_symbol = symbol
        symbol = KucoinEhdtdAuxClass.get_symbol_from_unified_symbol(unified_symbol)

        url_base = KucoinEhdtdAuxClass.get_api_url(trading_type=trading_type)
        default_endpoint = '/api/v1/market/candles'

        interval = KucoinEhdtdAuxClass.get_interval_from_unified_interval(interval)

        url = f'{url_base}{default_endpoint}?'
        url += f'symbol={symbol}&type={interval}'
        url += '&startAt='

        __delta_time = cls.get_delta_time_from_interval(interval)

        current_time = int(round(time.time()))

        start_time = 0
        first_time = None
        start_time = int(round(datetime.datetime(__min_history_year,\
                                                 __min_history_month,\
                                                 1,0,0,0,0).timestamp()))

        req_data = None

        while first_time is None and start_time < current_time:
            end_time = start_time + (__delta_time * 900)

            if end_time > time.time():
                end_time = int(time.time())

            if end_time < start_time:
                start_time = end_time - (__delta_time * 900)

            url_req = f'{url}{start_time}&endAt={end_time}'

            __attemp = -1
            __max_attemp = 9
            req_data = None
            post_data = None
            headers = None

            while __attemp < __max_attemp\
                and not (req_data is not None and isinstance(req_data, dict)):
                req_data = ecf.file_get_contents_url(url_req, 'r', post_data, headers)
                if req_data is None:
                    time.sleep(0.1)
                else:
                    if ecf.is_json(req_data):
                        req_data = json.loads(req_data)
                __attemp += 1

                if req_data is not None and isinstance(req_data, dict)\
                    and 'code' in req_data and 'data' in req_data\
                    and req_data['code'] is not None and int(req_data['code']) == 200000:
                    __data = req_data['data']

                    if __data is not None and isinstance(__data, list) and len(__data) > 0:
                        try:
                            first_time = int(round(int(__data[0][0])))

                        except Exception: # pylint: disable=broad-except
                            first_time = None

            if first_time is None:
                start_time = KucoinEhdtdAuxClass.get_next_month_time_from_time(start_time)

            time.sleep(0.1)

        if first_time is None:
            result = (int(time.strftime("%Y", time.gmtime(current_time))),\
                        int(time.strftime("%m", time.gmtime(current_time))))

        else:
            first_time = KucoinEhdtdAuxClass.get_next_month_time_from_time(int(first_time))

            result = (int(time.strftime("%Y", time.gmtime(first_time))),\
                        int(time.strftime("%m", time.gmtime(first_time))))

        return result

    @classmethod
    def not_daily_data(cls):
        """
        Ehdtd get_supported_intervals function.
        =======================================
            This method return a list of not daily data for this intervals.
                :param cls: KucoinEhdtdAuxClass Class.

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
                :param cls: KucoinEhdtdAuxClass Class.

                :return: list of supported intervals.
        """

        __result = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',\
                    '6h', '8h', '12h', '1d', '1w', '1mo']

        return __result
