#!/usr/bin/python
"""
Ehdtd - cryptoCurrency Exchange history data to database
Only for tester some code.

Author: Ricardo Marcelo Alvarez
Date: 2023-11-23
"""

import sys # pylint: disable=unused-import
import time # pylint: disable=unused-import
import datetime # pylint: disable=unused-import
import os # pylint: disable=unused-import
import logging # pylint: disable=unused-import
import json # pylint: disable=unused-import
import math # pylint: disable=unused-import

import pprint # pylint: disable=unused-import

from sympy import fibonacci

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """

    result = False

    phi = (1 + math.sqrt(5)) / 2

    n = 23

    fibs = []

    fibs_p = []

    for i in range(0, n+1):
        if i == 0:
            fibs_p.append(round(100 / math.pow(phi, 1/2), 4))
        else:
            fibs_p.append(round(100 / math.pow(phi, i), 4))

    for i in range(0, n+1):
        fibs.append(fibonacci(i))

    for i in range(0, len(fibs)): # pylint: disable=consider-using-enumerate
        fib_p = 0
        if i < (len(fibs) - 1):
            if fibs[i] > 0:
                fib_p = fibs[i+1] / fibs[i]
            fib_p = round(fib_p, 4)

        print(f'{i} -> {fibs[i]} -> {fib_p} -> {fibs_p[i]}')


    return result


if __name__ == "__main__":
    main(sys.argv[1:])
