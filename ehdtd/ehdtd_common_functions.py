"""
Ehdtd - cryptoCurrency Exchange history data to database
Common functions used by the package

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import urllib.parse
import urllib.request
import json
import zipfile
import io
import time
import datetime

def is_json(myjson):
    """
    is_json
    =======
        This function get a string or bytes and check if json return True
        if the input is a json valid string.
            :param myjson: str | bytes.

            :return bool: Return True if myjson is a str and is a json. 
    """

    result = False

    if myjson is not None and isinstance(myjson,(str,bytes)):
        try:
            json.loads(myjson)
            result = True
        except Exception: # pylint: disable=broad-except
            result = False

    return result

def file_put_contents(filename, data, mode_in=""):
    """
    file_put_contents
    =================
        This function put data into filename
            :param filename: str file path.
            :param data: str
            :param mode: str 'b' for binary mode.

            :return bool: 
    """
    result = False
    mode = "w"

    if len(mode_in) > 0:
        mode = mode_in
    try:
        f = open(filename, mode, encoding='utf-8')
        result = f.write(data)
        f.close()
    except Exception: # pylint: disable=broad-except
        result = False

    return result

def file_get_contents_url(url, mode='b', post_data=None, headers=None, timeout=90):
    """
    file_get_contents_url
    =====================
        This function get a url and reads into a string
            :param url: str file URL.
            :param mode: str 'b' for binary response.
            :param post_data: dict Post data in format key -> value.
            :param headers: dict headers in format key -> value.
            :param timeout: int request timeout.

            :return str: Return response data from url. 
    """

    result = None

    if headers is None:
        headers = {}

    try:
        req = None
        if post_data is not None and isinstance(post_data,dict):
            req = urllib.request.Request(url, urllib.parse.urlencode(post_data).encode(), headers)
        else:
            req = urllib.request.Request(url, None, headers)

        if req is not None:
            try:
                with urllib.request.urlopen(req, None, timeout=timeout) as response:
                    result = response.read()

            except Exception: # pylint: disable=broad-except
                result = None

        if mode != 'b' and result is not None and isinstance(result, bytes):
            result = result.decode()

    except Exception: # pylint: disable=broad-except
        result = None

    if mode != 'b' and result is not None and result is not False and isinstance(result, bytes):
        result = result.decode()

    return result

def file_get_contents_url_cmpl(url, mode='b', post_data=None, headers=None, timeout=900):
    """
    file_get_contents_url
    =====================
        This function get a url and reads into a string
            :param url: str file URL.
            :param mode: str 'b' for binary response.
            :param post_data: dict Post data in format key -> value.
            :param headers: dict headers in format key -> value.
            :param timeout: int request timeout.

            :return dict: Return response data from url. 
    """

    result = {}
    result['code'] = None
    result['data'] = None
    result['headers'] = None
    result['headers_str'] = None
    result['final_url'] = None
    result['exception_status'] = False
    result['exception_code'] = None
    result['exception'] = None

    if headers is None:
        headers = {}

    try:
        req = None
        if post_data is not None and isinstance(post_data,dict):
            req = urllib.request.Request(url, urllib.parse.urlencode(post_data).encode(), headers)
        else:
            req = urllib.request.Request(url, None, headers)

        if req is not None:
            try:
                with urllib.request.urlopen(req, None, timeout=timeout) as response:
                    result['code'] = response.status
                    result['data'] = response.read()
                    result['headers'] = response.headers
                    result['headers_str'] = response.headers.as_string()
                    result['final_url'] = response.url

            except Exception as exc: # pylint: disable=broad-except
                result['exception_status'] = True
                if hasattr(exc, 'code'):
                    result['exception_code'] = exc.code
                result['exception'] = exc

        if mode != 'b' and result['data'] is not None and isinstance(result['data'], bytes):
            result['data'] = result['data'].decode()

    except Exception as exc: # pylint: disable=broad-except
        result['exception_status'] = True
        if hasattr(exc, 'code'):
            result['exception_code'] = exc.code
        result['exception'] = exc

    if mode != 'b' and result['data'] is not None\
        and result['data'] is not False\
        and isinstance(result['data'], bytes):
        result['data'] = result['data'].decode()

    return result

def decompress_zip_data(data):
    """
    decompress_zip_data
    ===================
        This function get a url and reads into a string
            :param data: bytes

            :return str: Return decompress data. 
    """

    result = None

    try:
        # z = zipfile.ZipFile(io.BytesIO(data))
        # result = z.read(z.infolist()[0]).decode()

        with zipfile.ZipFile(io.BytesIO(data)) as z:
            result = z.read(z.infolist()[0]).decode()

    except Exception: # pylint: disable=broad-except
        result = None

    return result

def months_ago_counter(from_year, from_month):
    """
    months_ago_counter
    ==================
        This function get a year and month and return months ago
            :param from_year: int
            :param from_month: int

            :return int: months ago
    """

    result = 0

    __this_time = round(time.time())
    __this_year = int(time.strftime("%Y",time.gmtime(__this_time)))
    __this_month = int(time.strftime("%m",time.gmtime(__this_time)))

    date_from = datetime.datetime(from_year, from_month, 1)
    date_to = datetime.datetime(__this_year, __this_month, 1)

    result = (date_to.year - date_from.year) * 12 + date_to.month - date_from.month

    return result
