"""
Ehdtd - cryptoCurrency Exchange history data to database
Binance auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import json
import time
import io
import csv
import random
import hashlib
import calendar

import ehdtd.ehdtd_common_functions as ecf

class BinanceEhdtdAuxClass():
    """
    Ehdtd - cryptoCurrency Exchange history data to database BinanceEhdtdAuxClass
    =============================================================================
        This class contains helper functions for the Ehdtd class.

        For Binance exchange de data is available here
            Data in this link https://github.com/binance/binance-public-data/#trades-1
            Data begin in year 2017 but it is convenient start form 2018
    """

    def __init__(self):
        """
        BinanceEhdtdAuxClass constructor
        ================================
            Initializes the BinanceEhdtdAuxClass with the provided parameters.

                :param self: BinanceEhdtdAuxClass instance.
                :return: Return a new instance of the Class BinanceEhdtdAuxClass.
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

            __l_url_api = BinanceEhdtdAuxClass.get_api_url()
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

    def get_historical_data_from_url_file(self, symbol, interval, year, month, day=None,\
                                          force_daily=False, trading_type='SPOT'):
        """
        get_historical_data_from_url_file
        =================================
            For Binance exchange de data is available here
                Data in this link https://github.com/binance/binance-public-data/#trades-1
                Data begin in year 2017 but it is convenient start form 2018

            :param self: BinanceEhdtdAuxClass instance.
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
        data_csv = ''

        last_day = calendar.monthrange(year, month)[1]

        if day is None:
            day = 1

        __this_time = round(time.time())
        __this_year = int(time.strftime("%Y",time.gmtime(__this_time)))
        __this_month = int(time.strftime("%m",time.gmtime(__this_time)))
        __this_day = int(time.strftime("%d",time.gmtime(__this_time)))
        __this_hour = int(time.strftime("%H",time.gmtime(__this_time)))

        day_start = 1

        if year == __this_year and month == __this_month:
            last_day = __this_day
            day_start = day

        time_type = 'monthly'

        unified_symbol = BinanceEhdtdAuxClass.get_symbol_from_unified_symbol(symbol)

        url_base_month = 'https://data.binance.vision/data/' + trading_type.lower() + '/'\
            + str(time_type)\
            + '/klines/'\
            + unified_symbol\
            + '/' + str(interval) + '/'

        file_get_month = unified_symbol\
            + '-' + str(interval) + '-'\
            + str(year) + '-' + str(month).zfill(2) + '.zip'

        url_month = url_base_month + file_get_month

        url_days = []

        time_type = 'daily'

        have_daily_data = interval not in BinanceEhdtdAuxClass.not_daily_data()

        if have_daily_data:
            url_base_days = 'https://data.binance.vision/data/' + trading_type.lower() + '/'\
                + str(time_type)\
                + '/klines/'\
                + unified_symbol\
                + '/' + str(interval) + '/'

            for day_get in range(day_start, last_day):
                cust_test = year == __this_year and month == __this_month\
                    and day_get >= (last_day - 1) and __this_hour <= 14

                if not cust_test:
                    file_get_days = unified_symbol\
                        + '-' + str(interval) + '-'\
                        + str(year) + '-' + str(month).zfill(2)\
                        + '-' + str(day_get).zfill(2) + '.zip'
                    url_day = url_base_days + file_get_days

                    url_days.append(url_day)

        url_checksum = url_month + '.CHECKSUM'

        if not (year == __this_year\
                and month == __this_month\
                and __this_day < 8):
            data = self.__process_url_data(url_month, url_checksum)

            if data is not None:
                data_csv += data
                result = []
        else:
            if result is not None:
                result = []

        if (len(data_csv) == 0 and len(url_days) > 0) or\
            (force_daily and interval not in BinanceEhdtdAuxClass.not_daily_data()):
            for url_day in url_days:
                url_checksum = url_day + '.CHECKSUM'
                data = self.__process_url_data(url_day, url_checksum)

                if data is not None:
                    data_csv += data
                    if result is None:
                        result = []

        if len(data_csv) > 0:

            fieldnames = ['open_time', 'open_price', 'high', 'low', 'close_price',\
                          'volume', 'close_time', 'xxx_007', 'xxx_008', 'xxx_009',\
                          'xxx_010', 'xxx_014']

            reader = csv.DictReader(io.StringIO(data_csv), fieldnames)

            for row in reader:

                open_time = int(round(int(row['open_time'])/1000))

                data_line = None
                data_line = {}
                data_line['open_time'] = open_time
                data_line['open_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(open_time))
                )
                data_line['open_price'] = row['open_price']
                data_line['close_time'] = int(round(int(row['close_time'])/1000))
                data_line['close_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(data_line['close_time']))
                )
                data_line['close_price'] = row['close_price']
                data_line['low'] = row['low']
                data_line['high'] = row['high']
                data_line['volume'] = row['volume']
                data_line['exchange'] = 'binance'
                data_line['symbol'] = symbol
                data_line['interval'] = interval
                result.append(data_line)

        return result

    def __process_url_data(self, url, url_checksum):
        result = None

        data = ecf.file_get_contents_url_cmpl(url)
        data_checksum = ecf.file_get_contents_url_cmpl(url_checksum)

        if data is not None and data_checksum is not None \
                and isinstance(data, dict) and isinstance(data_checksum, dict):

            if not data['exception_status'] and not data_checksum['exception_status']:

                if int(data['code']) == 200 and int(data_checksum['code']) == 200:

                    checksum = data_checksum['data'].decode('utf-8').split()[0]
                    data_zip_checksum = hashlib.sha256(data['data']).hexdigest()

                    if checksum == data_zip_checksum:
                        result = ecf.decompress_zip_data(data['data'])

        return result

    def get_last_klines_candlestick_data(self, symbol, interval, start_time=None, limit=1000):
        """
        Ehdtd get_last_klines_candlestick_data function.
        ================================================
            This method return a list of last klines data.
                :param self: BinanceEhdtdAuxClass instance.
                :param symbol: str
                :param interval: str
                :param start_time: int unix timestamp if is None start_time is time.time() - 900
                :param limit: int if limit is greater than 1000, 1000 is asigned

                :return: list[dict] of klines
        
        """
        result = None

        result = BinanceEhdtdAuxClass.get_kline_data(symbol,\
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
                :param cls: BinanceEhdtdAuxClass Class.
                :param trading_type: str only allowed 'SPOT'
                :return str: Return API URL.
        """
        result = None

        if trading_type == 'SPOT':
            __url_api = 'https://api.binance.com/api/v3'
            #__url_test = 'https://testnet.binance.vision/api/v3'

        result = __url_api

        return result

    @classmethod
    def get_unified_symbol_from_symbol(cls, symbol):
        """
        get_unified_symbol_from_symbol
        ==============================
            This function get unified symbol from symbol.
                :param cls: BinanceEhdtdAuxClass Class.
                :param symbol: str.
                :return str: Return unified symbol.
        """
        result = symbol
        beac = BinanceEhdtdAuxClass()

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
                :param cls: BinanceEhdtdAuxClass Class.
                :param symbol: str unified symbol.
                :return str: Return symbol.
        """
        result = symbol

        beac = BinanceEhdtdAuxClass()

        if symbol is not None\
            and isinstance(symbol, str)\
            and len(symbol) > 0\
            and beac.if_symbol_supported(symbol):
            result = symbol.replace('/', '').upper()
            result = str(result)

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
                :param cls: BinanceEhdtdAuxClass Class.
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
        symbol = BinanceEhdtdAuxClass.get_symbol_from_unified_symbol(symbol)

        if default_endpoint not in ['klines', 'uiKlines']:
            default_endpoint = 'uiKlines'

        url_base = BinanceEhdtdAuxClass.get_api_url(trading_type=trading_type)
        url = f'{url_base}/{default_endpoint}?symbol={symbol}&interval={interval}&limit={limit}'

        start_time_out = ''
        end_time_out = ''

        if start_time is not None and (isinstance(start_time, (int,float)) or start_time == 0):
            start_time = round(start_time * 1000)
            start_time_out = f'&startTime={start_time}'

        if end_time is not None and (isinstance(end_time, (int,float)) or end_time == 0):
            end_time = round(end_time * 1000)
            end_time_out = f'&endTime={end_time}'

        url = url + start_time_out + end_time_out

        time.sleep(round(random.uniform(0.1, 0.25), 1))

        __attemp = 0
        __max_attemp = 9
        req_data = ecf.file_get_contents_url_cmpl(url, mode='r')

        while __attemp < __max_attemp and not (req_data is not None and isinstance(req_data, dict)):
            req_data = ecf.file_get_contents_url_cmpl(url, mode='r')
            __attemp += 1
            time.sleep(round(random.uniform(4, 5), 1))

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
                            data_line['exchange'] = 'binance'
                            data_line['symbol'] = unified_symbol
                            data_line['interval'] = interval
                            result.append(data_line)

                    if len(result) > 0:
                        result = result[:-1]

        return result

    @classmethod
    def get_symbol_first_year_month_listed(cls, symbol, interval, trading_type: str='SPOT'):
        """
        get_symbol_first_year_month_listed
        ==================================
            This function set and return API URL.
                :param cls: BinanceEhdtdAuxClass Class.
                :param symbol: str
                :param interval: str
                :param trading_type: str only allowed 'SPOT'
                :return tuple: Return a tuple first element is first year listed\
                               and second element is first month listed
        """
        __min_history_year = 2018
        __min_history_month = 1

        result = (__min_history_year, __min_history_month)

        data = BinanceEhdtdAuxClass.get_kline_data(symbol,\
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
                :param cls: BinanceEhdtdAuxClass Class.

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
                :param cls: BinanceEhdtdAuxClass Class.

                :return: list of supported intervals.
        """
        __result = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',\
                    '6h', '8h', '12h', '1d', '3d', '1w', '1mo']

        return __result
