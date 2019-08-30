# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Simple progressbar formatting."
import sys
from deprecation import deprecated


@deprecated(deprecated_in="0.8", removed_in="1.0")
def with_progressbar(iterable, total=None, width=120, desc='',
                     percentage=True, file=None):
    if file is None:
        file = sys.stderr
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
            file.write('\r' * width)
        bar = left + '#' * f + '-' * n + right
        file.write(bar.format(p=p))
        file.flush()

    try:
        for i, item in enumerate(iterable):
            if i % n == 0:
                _draw(i/total)
            yield item
        _draw(1.0)
    finally:
        file.write('\n')
        file.flush()


__all__ = ['with_progressbar']
