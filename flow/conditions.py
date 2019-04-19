# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.


class Condition(object):

    def __init__(self, condition):
        self.condition = condition

    @classmethod
    def isfile(cls, filename):
        return cls(lambda job: job.isfile(filename))

    @classmethod
    def true(cls, key):
        return cls(lambda job: job.document.get(key, False))

    @classmethod
    def false(cls, key):
        return cls(lambda job: not job.document.get(key, False))

    @classmethod
    def always(cls, func):
        return cls(lambda _: True)(func)

    @classmethod
    def never(cls, func):
        return cls(lambda _: False)(func)

    @classmethod
    def not_(cls, condition):
        return cls(lambda job: not condition(job))
