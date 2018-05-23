# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Simple progressbar formatting."
from __future__ import print_function
import sys


def with_progressbar(iterable, total=None, width=120, desc='',
                     percentage=True, file=sys.stderr):
    if total is None:
        total = len(iterable)
    n = max(1, total // width)

    left = desc + '|'
    right = '|{p:>4.0%}' if percentage else '|'
    w = width - len(left) - len(right)

    def _draw(p):
        f = int(p*w)
        n = w-f
        if p:
            print('\r' * width, end='', file=file)
        bar = left + '#' * f + '-' * n + right
        print(bar.format(p=p), end='\n' if p >= 1 else '', file=file)

    for i, item in enumerate(iterable):
        if i % n == 0:
            _draw(i/total)
        yield item
    _draw(1.0)


__all__ = ['with_progressbar']
