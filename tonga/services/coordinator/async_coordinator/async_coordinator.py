#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any

from asyncio import Event, Future, get_event_loop, AbstractEventLoop

__all__ = [
    'DictIsFull',
    'DictIsEmpty',
    'AsyncCoordinator'
]


class DictIsFull(Exception):
    pass


class DictIsEmpty(Exception):
    pass


class AsyncCoordinator:
    _loop: AbstractEventLoop
    _maxsize: int
    _getters: Dict[str, Future]
    _putters: Dict[str, Future]
    _unfinished_tasks: int
    _finished: Event

    _dict: Dict[str, Any]

    def __init__(self, maxsize=0, *, loop=None):
        if loop is None:
            self._loop = get_event_loop()
        else:
            self._loop = loop
        self._maxsize = maxsize

        self._getters: Dict[str, Future] = dict()
        self._putters: Dict[str, Future] = dict()

        self._unfinished_tasks = 0
        self._finished = Event(loop=self._loop)
        self._finished.set()
        self._dict = dict()

    def get_dict_copy(self) -> Dict[str, Any]:
        return self._dict.copy()

    def _get(self, key: str) -> Any:
        value = self._dict[key]
        del self._dict[key]
        return value

    def _put(self, key: str, value: Any) -> None:
        self._dict[key] = value

    def _wake_up_getter(self, key: str) -> None:
        if key in self._getters:
            waiter = self._getters[key]
            if not waiter.done():
                waiter.set_result(None)

    def _wake_up_putter(self, key: str) -> None:
        if key in self._putters:
            waiter = self._putters[key]
            if not waiter.done():
                waiter.set_result(None)

    def __repr__(self) -> str:
        return f'<{type(self).__name__} at {id(self):#x} {self._format()}>'

    def __str__(self) -> str:
        return f'<{type(self).__name__} {self._format()}>'

    def _format(self) -> str:
        result = f'maxsize={self._maxsize!r}'
        if getattr(self, '_dict', None):
            result += f' _dict={dict(self._dict)!r}'
        if self._getters:
            result += f' _getters[{len(self._getters)}]'
        if self._putters:
            result += f' _putters[{len(self._putters)}]'
        if self._unfinished_tasks:
            result += f' tasks={self._unfinished_tasks}'
        return result

    def qsize(self) -> int:
        return len(self._dict)

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def empty(self) -> bool:
        return not self._dict

    def full(self) -> bool:
        if self._maxsize <= 0:
            return False
        else:
            return self.qsize() >= self._maxsize

    async def put(self, key: str, value: Any) -> None:
        while self.full():
            putter = self._loop.create_future()
            self._putters[key] = putter
            try:
                await putter
            except Exception as err:
                print('Put err = ', err)
                # Just in case putter is not done yet.
                putter.cancel()
                try:
                    # Clean self._putters from canceled putters.
                    del self._putters[key]
                except ValueError:
                    # The putter could be removed from self._putters by a
                    # previous get_nowait call.
                    pass
                if not self.full() and not putter.cancelled():
                    # We were woken up by get_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wake_up_putter(key)
                raise
        return self.put_nowait(key, value)

    def put_nowait(self, key: str, value: Any) -> None:
        print(f'put no wait {key}, {value}')
        if self.full():
            raise DictIsFull
        self._put(key, value)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wake_up_getter(key)

    async def get(self, key: str) -> Any:
        print('dict = ',  self._dict)
        print('loop = ', self._loop)
        while self._dict[key]:
            print('self._dict[key] = ', self._dict[key])
            getter = self._loop.create_future()
            self._getters[key] = getter
            try:
                print('Before await')
                await getter
                print('After await')
            except Exception as err:
                print('Err = ', err)
                # Just in case getter is not done yet.
                getter.cancel()
                try:
                    # Clean self._getters from canceled getters.
                    del self._getters[key]
                except ValueError:
                    # The getter could be removed from self._getters by a
                    # previous put_nowait call.
                    pass
                if not self.empty() and not getter.cancelled():
                    # We were woken up by put_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wake_up_getter(key)
                raise
        return self.get_nowait(key)

    def get_nowait(self, key: str) -> Any:
        if self.empty():
            raise DictIsEmpty
        item = self._get(key)
        self._wake_up_putter(key)
        return item

    def task_done(self) -> None:
        if self._unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    async def join(self) -> None:
        if self._unfinished_tasks > 0:
            await self._finished.wait()
