# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
import time
import queue
import itertools
from threading import Thread, Event
from math import tanh
from itertools import count

from pymongo import ASCENDING


logger = logging.getLogger(__name__)

ID_COUNTER = 0
KEY_COUNTER = 'counter'
FILTER_COUNTER = {'_id': ID_COUNTER}
FILTER_COUNTER_DEC = {'_id': ID_COUNTER, KEY_COUNTER: {'$gt': 0}}
FILTER_NOT_COUNTER = {'_id': {'$ne': ID_COUNTER}}


def _fetch(target, timeout=None, stop_event=None):
    tmp_queue = queue.Queue()
    if stop_event is None:
        stop_event = Event()

    def inner_loop():
        w = (tanh(0.05 * i) for i in itertools.count())
        while(not stop_event.is_set()):
            result = target()
            if result is not None:
                tmp_queue.put(result)
                return
        stop_event.wait(max(0.001, next(w)))
    thread__fetch = Thread(target=inner_loop)
    thread__fetch.start()
    try:
        thread__fetch.join(timeout=timeout)
    except KeyboardInterrupt:
        stop_event.set()
        thread__fetch.join()
        raise
    if thread__fetch.is_alive():
        stop_event.set()
        thread__fetch.join()
    try:
        return tmp_queue.get_nowait()
    except queue.Empty:
        raise TimeoutError()


class Empty(Exception):
    pass


class Full(Exception):
    pass


class MongoDBQueue(object):

    def __init__(self, collection):
        self._collection = collection

    def qsize(self):
        return self._collection.find(FILTER_NOT_COUNTER).count()

    def empty(self):
        return self.qsize() == 0

    def full(self):
        return False  # There is no limit on the queue size implemented.

    def _get(self):
        return self._collection.find_one_and_delete(
            FILTER_NOT_COUNTER, sort=[('_id', ASCENDING)])

    def __contains__(self, item):
        return self._collection.find_one(item) is not None

    def _num_open_tasks(self):
        result = self._collection.find_one(FILTER_COUNTER)
        if result is None:
            return 0
        else:
            open_tasks = int(result[KEY_COUNTER])
            if open_tasks < 0:
                raise ValueError()
            else:
                return open_tasks

    def put(self, item, block=True, timeout=None):
        # block and timeout are ignored, as in this implementation, the queue
        # can never be full.
        result = self._collection.insert_one(item)
        self._collection.update_one(
            FILTER_COUNTER, {'$inc': {KEY_COUNTER: 1}}, upsert=True)
        return result.inserted_id

    def get(self, block=True, timeout=None, stop_event=None):
        if block:
            try:
                return _fetch(
                    target=self._get,
                    timeout=timeout,
                    stop_event=stop_event)
            except TimeoutError:
                raise Empty()
        else:
            item = self._get()
            if item is None:
                raise Empty()
            else:
                return item

    def get_nowait(self):
        return self.get(block=False)

    def task_done(self):
        result = self._collection.update_one(
            FILTER_COUNTER_DEC, {'$inc': {KEY_COUNTER: -1}})
        if result.modified_count != 1:
            raise ValueError()

    def join(self):
        w = (tanh(0.05 * i) for i in count())
        while True:
            if self._num_open_tasks() == 0:
                return
            time.sleep(max(0.001, next(w)))

    def peek(self):
        for item in self._collection.find(FILTER_NOT_COUNTER):
            yield item

    def clear(self):
        self._collection.delete_many(
            {'$or': [FILTER_COUNTER, FILTER_NOT_COUNTER]})
