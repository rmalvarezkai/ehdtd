"""
Ehdtd - cryptoCurrency Exchange history data to database

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

from os.path import dirname, basename, isfile, join
import glob
import sys
import importlib.metadata

from .ehdtd import Ehdtd, EhdtdRO, EhdtdExchangeConfig

ehdtd_metadata = importlib.metadata.metadata('ehdtd')

__title__ = ehdtd_metadata['Name']
__summary__ = ehdtd_metadata['Summary']
__uri__ = ehdtd_metadata['Home-page']
__version__ = ehdtd_metadata['Version']
__author__ = ehdtd_metadata['Author']
__email__ = ehdtd_metadata['Author-email']
__license__ = ehdtd_metadata['License']
__copyright__ = 'Copyright © 2023 Ricardo Marcelo Alvarez'

modules = glob.glob(join(dirname(__file__), "*.py"))
__all__ = []

if isinstance(sys.path,list):
    sys.path.append(dirname(__file__))

for f in modules:
    if isfile(f) and not f.endswith('__init__.py'):
        __all__.append(basename(f)[:-3])

##from .binance import BinanceAuxClass
