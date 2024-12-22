"""
Ehdtd - cryptoCurrency Exchange history data to database
Okx auxiliary functions

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

class OkxEhdtdAuxClass():
    """
    Ehdtd - cryptoCurrency Exchange history data to database OkxEhdtdAuxClass
    =============================================================================
        This class contains helper functions for the Ehdtd class.

    """

    def __init__(self):
        """
        OkxEhdtdAuxClass constructor
        ============================
            Initializes the OkxEhdtdAuxClass with the provided parameters.

                :param self: OkxEhdtdAuxClass instance.
                :return: Return a new instance of the Class OkxEhdtdAuxClass.
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

            __l_url_api = OkxEhdtdAuxClass.get_api_url()
            __l_endpoint = '/api/v5/public/instruments'
            __l_url_point = f'{__l_url_api}{__l_endpoint}?'
            __l_url_point += f'instType={trading_type.upper()}'

            post_data = None
            headers = {
                'User-Agent': 'Ehdtd - cryptoCurrency Exchange history data to database'
            }

            __data = ecf.file_get_contents_url(__l_url_point, 'r', post_data, headers)

            if __data is not None and ecf.is_json(__data):
                __data = json.loads(__data)
                if __data is not None and isinstance(__data, dict)\
                    and 'code' in __data and 'data' in __data\
                    and int(__data['code']) == 0:
                    result = __data
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

        if __main_data is not None and isinstance(__main_data, dict)\
            and __main_data['data'] is not None\
            and isinstance(__main_data['data'], list):

            try:
                result = []
                for symbol_data in __main_data['data']:
                    if symbol_data is not None and isinstance(symbol_data, dict):
                        if 'baseCcy' in symbol_data and 'quoteCcy' in symbol_data\
                            and isinstance(symbol_data['baseCcy'],str)\
                            and isinstance(symbol_data['quoteCcy'],str):
                            result.append(\
                                symbol_data['baseCcy'].upper() + '/'\
                                    + symbol_data['quoteCcy'].upper())

                if sort_list:
                    result.sort()

            except Exception: # pylint: disable=broad-except
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

            :param self: OkxEhdtdAuxClass instance.
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
                :param self: OkxEhdtdAuxClass instance.
                :param symbol: str
                :param interval: str
                :param start_time: int unix timestamp if is None start_time is time.time() - 900
                :param limit: int if limit is greater than 1000, 1000 is asigned

                :return: list[dict] of klines
        
        """
        result = None

        result = OkxEhdtdAuxClass.get_kline_data(symbol,\
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
                :param cls: OkxEhdtdAuxClass Class.
                :param trading_type: str only allowed 'SPOT'
                :return str: Return API URL.
        """
        result = None

        __url_api = None
        if trading_type == 'SPOT':
            __url_api = 'https://www.okx.com'

        result = __url_api

        return result

    @classmethod
    def get_exchange_connectivity(cls):
        """
        get_exchange_connectivity
        =========================
            This function return a dict with connectivity information.
                :param cls: OkxEhdtdAuxClass Class.
                :return dict: result.
                    result = {
                        'result': bool, # True if connectivity is working False in other case.
                        'code': int | None, # Error Code
                        'msg': str | None # Error message
                    }
        """
        result = None

        headers = {
            'User-Agent': 'Ehdtd - cryptoCurrency Exchange history data to database'
        }

        timeout = 45

        __url_api = cls.get_api_url()
        __endpoint = '/api/v5/public/time'
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

            if result is not None:
                if ecf.is_json(result['data']):
                    result['data'] = json.loads(result['data'])

                    if result['data'] is not None and isinstance(result['data'], dict):
                        if 'code' in result['data']:
                            result['res_code'] = int(result['data']['code'])
                        if 'msg' in result['data']:
                            result['res_msg'] = result['data']['msg']
                            if isinstance(result['data']['msg'], bytes):
                                result['res_msg'] = result['data']['msg'].decode()

                if result['code'] is not None\
                    and isinstance(result['code'], int)\
                    and 200 <= result['code'] < 300\
                    and result['res_code'] is not None\
                    and result['res_code'] == 0:
                    result['result'] = True
                    result['headers'] = None
                    result['res_code'] = None
                    result['res_msg'] = None
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
                :param cls: OkxEhdtdAuxClass Class.
                :param symbol: str.
                :return str: Return unified symbol.
        """
        result = symbol
        beac = OkxEhdtdAuxClass()

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
                :param cls: OkxEhdtdAuxClass Class.
                :param symbol: str unified symbol.
                :return str: Return symbol.
        """
        result = symbol

        beac = OkxEhdtdAuxClass()

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
                       limit: int=100,\
                       default_endpoint: str='uiKlines',\
                       trading_type: str='SPOT'):
        """
        get_kline_data
        ==============
            This function get the kline/candlestick API URL.
                :param cls: OkxEhdtdAuxClass Class.
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
        symbol = OkxEhdtdAuxClass.get_symbol_from_unified_symbol(symbol)

        url_base = OkxEhdtdAuxClass.get_api_url(trading_type=trading_type)

        req_interval = OkxEhdtdAuxClass.get_interval_from_unified_interval(interval)

        start_time_out = ''
        end_time_out = ''

        __delta_time = cls.get_delta_time_from_interval(interval)

        __last_data = False

        if start_time is not None and isinstance(start_time, (int,float)):
            start_time = max(start_time, 0)
            start_time_cmp = round(time.time()) - (250 * __delta_time)

            if start_time >= start_time_cmp:
                __last_data = True

            start_time = int(round(start_time))
            start_time_out = f'&before={start_time}000'

        if __last_data:
            limit = min(limit, 300)
        else:
            limit = min(limit, 100)

        if end_time is not None\
            and isinstance(end_time, (int, float, str))\
            and end_time >= 0\
            and end_time > start_time:
            end_time = int(round(end_time * 1000))
            end_time_out = f'&after={end_time}'
        elif len(start_time_out) > 0:
            end_time = start_time + (__delta_time * limit)
            end_time_out = f'&after={int(end_time)}000'

        default_endpoint = '/api/v5/market/history-candles'
        if __last_data:
            default_endpoint = '/api/v5/market/candles'


        url = f'{url_base}{default_endpoint}'
        url += f'?instType={trading_type.upper()}&instId={symbol}'
        url += f'&bar={req_interval}&limit={limit}'

        url += f'{start_time_out}{end_time_out}'

        __attemp = -1
        __max_attemp = 9
        req_data = None
        post_data = None
        headers = {
            'User-Agent': 'Ehdtd - cryptoCurrency Exchange history data to database'
        }

        while __attemp < __max_attemp and not (req_data is not None and isinstance(req_data, dict)):
            req_data = ecf.file_get_contents_url(url, 'r', post_data, headers)
            if ecf.is_json(req_data):
                req_data = json.loads(req_data)
            time.sleep(0.1)
            __attemp += 1

        if req_data is not None and isinstance(req_data, dict):

            try:
                __data = req_data['data']

                if __data is not None and isinstance(__data, list):
                    result = []
                    __delta_time = cls.get_delta_time_from_interval(interval)

                    for kline in __data:
                        if kline is not None and isinstance(kline, list) and len(kline) >= 8:
                            data_line = None
                            data_line = {}
                            data_line['open_time'] = int(round(int(kline[0])/1000))
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
                            data_line['close_price'] = kline[4]
                            data_line['low'] = kline[3]
                            data_line['high'] = kline[2]
                            data_line['volume'] = kline[5]
                            data_line['exchange'] = 'okx'
                            data_line['symbol'] = unified_symbol
                            data_line['interval'] = interval

                            if int(kline[8]) == 1:
                                result.append(data_line)

                    if len(result) > 1:
                        result.reverse()

            except Exception as e: # pylint: disable=broad-except
                print(f'ERROR: {e}')
                result = None

        return result

    @classmethod
    def get_interval_from_unified_interval(cls, interval):
        """
        get_interval_from_unified_interval
        ==================================

        """
        result = None

        # e.g. [1s/1m/3m/5m/15m/30m/1H/2H/4H]
        # Hong Kong time opening price k-line: [6H/12H/1D/2D/3D/1W/1M/3M]
        # UTC time opening price k-line: [6Hutc/12Hutc/1Dutc/2Dutc/3Dutc/1Wutc/1Mutc/3Mutc]
        __intervals_map = {
            '1m': '1m',
            '3m': '3m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1H',
            '2h': '2H',
            '4h': '4H',
            '6h': '6Hutc',
            '12h': '12Hutc',
            '1d': '1Dutc',
            '3d': '3Dutc',
            '1w': '1Wutc',
            '1mo': '1Mutc'
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
                :param cls: OkxEhdtdAuxClass Class.
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
        symbol = OkxEhdtdAuxClass.get_symbol_from_unified_symbol(unified_symbol)
        limit = 2

        url_base = OkxEhdtdAuxClass.get_api_url(trading_type=trading_type)
        default_endpoint = '/api/v5/market/history-candles'

        interval = OkxEhdtdAuxClass.get_interval_from_unified_interval(interval)

        url = f'{url_base}{default_endpoint}?'
        url += f'instType={trading_type.lower()}&instId={symbol}&bar={interval}&limit={limit}'
        url += '&after='

        current_time = int(round(time.time()))

        start_time = 0
        first_time = None
        start_time = int(round(datetime.datetime(__min_history_year,\
                                                 __min_history_month,\
                                                 1,0,0,0,0).timestamp()))

        req_data = None

        while first_time is None and start_time < current_time:

            url_req = f'{url}{start_time}000'

            __attemp = -1
            __max_attemp = 9
            req_data = None
            post_data = None
            headers = {
                'User-Agent': 'Ehdtd - cryptoCurrency Exchange history data to database'
            }

            while __attemp < __max_attemp\
                and not (req_data is not None and isinstance(req_data, dict)):
                req_data = ecf.file_get_contents_url(url_req, 'r', post_data, headers)
                if req_data is None:
                    time.sleep(0.1)
                else:
                    if ecf.is_json(req_data):
                        req_data = json.loads(req_data)
                __attemp += 1

            if req_data is not None and isinstance(req_data, dict):
                if 'code' in req_data\
                    and int(req_data['code']) == 0\
                    and 'data' in req_data\
                    and isinstance(req_data['data'], list):

                    try:
                        first_time = int(round(int(req_data['data'][-1][0]) / 1000))

                    except Exception: # pylint: disable=broad-except
                        first_time = None

            if first_time is None:
                start_time = OkxEhdtdAuxClass.get_next_month_time_from_time(start_time)
            time.sleep(0.1)

        if first_time is None:
            result = (int(time.strftime("%Y", time.gmtime(current_time))),\
                        int(time.strftime("%m", time.gmtime(current_time))))

        else:
            first_time = OkxEhdtdAuxClass.get_next_month_time_from_time(int(first_time))

            result = (int(time.strftime("%Y", time.gmtime(first_time))),\
                        int(time.strftime("%m", time.gmtime(first_time))))

        return result

    @classmethod
    def not_daily_data(cls):
        """
        Ehdtd get_supported_intervals function.
        =======================================
            This method return a list of not daily data for this intervals.
                :param cls: OkxEhdtdAuxClass Class.

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
                :param cls: OkxEhdtdAuxClass Class.

                :return: list of supported intervals.
        """

        __result = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',\
                    '6h', '12h', '1d', '3d','1w', '1mo']

        return __result
