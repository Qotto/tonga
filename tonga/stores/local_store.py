#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseLocalStore

"""

import asyncio
import functools
from asyncio import AbstractEventLoop, Task
from logging import getLogger
from typing import Dict, Any

from tonga.models.structs.persistency_type import PersistencyType
from tonga.services.coordinator.async_coordinator.async_coordinator import AsyncCoordinator
from tonga.stores.base import BaseStores
from tonga.stores.errors import StoreKeyNotFound, BadEntryType
from tonga.stores.manager.errors import UninitializedStore
from tonga.stores.persistency.memory import MemoryPersistency
from tonga.stores.persistency.shelve import ShelvePersistency
from tonga.stores.persistency.rocksdb import RocksDBPersistency
from tonga.models.structs.store_record_type import StoreRecordType

__all__ = [
    'LocalStore',
]


class LocalStore(BaseStores):
    """ Local stores
    """
    _lock: Dict[str, Any]
    _lock_coordinator: AsyncCoordinator
    _loop: AbstractEventLoop

    def __init__(self, db_type: PersistencyType, loop: AbstractEventLoop, db_path: str = None):
        self._logger = getLogger('tonga')
        self._lock = dict()
        self._loop = loop

        self._lock_coordinator = AsyncCoordinator(maxsize=0, loop=self._loop)

        if db_type == PersistencyType.MEMORY:
            self._persistency = MemoryPersistency()
        elif db_type == PersistencyType.SHELVE:
            if db_path is not None:
                self._persistency = ShelvePersistency(db_path)
            else:
                # Todo change raised error
                raise KeyError
        elif db_type == PersistencyType.ROCKSDB:
            if db_path is not None:
                self._persistency = RocksDBPersistency(db_path)
            else:
                # Todo change raised error
                raise KeyError
        else:
            # Todo change raised error
            raise NotImplementedError

    async def get(self, key: str) -> bytes:
        """ Get value by key in local store

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        if self._persistency.is_initialize():
            if isinstance(key, str):
                await self.__ready(key)
                try:
                    return await self._persistency.get(key)
                except StoreKeyNotFound:
                    raise StoreKeyNotFound
            else:
                raise BadEntryType
        raise UninitializedStore

    async def set(self, key: str, value: bytes) -> None:
        """ Set value & key in global store

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        if self._persistency.is_initialize():
            if isinstance(key, str) and isinstance(value, bytes):
                if key in self._lock:
                    if self._lock[key]:
                        await self.__ready(key)
                await self.__lock_entry(key)
                future = asyncio.ensure_future(self.__set_in_store(key, value), loop=self._loop)
                future.add_done_callback(functools.partial(self.__unlock_entry, key, False))
            else:
                raise BadEntryType
        else:
            raise UninitializedStore

    async def delete(self, key: str) -> None:
        """ Delete value by key in global store

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        if self._persistency.is_initialize():
            if isinstance(key, str):
                try:
                    if self._lock[key]:
                        await self.__ready(key)
                    await self.__lock_entry(key)
                    future = asyncio.ensure_future(self.__delete_in_store(key), loop=self._loop)
                    future.add_done_callback(functools.partial(self.__unlock_entry, key, True))
                except (KeyError, ValueError, TypeError) as err:
                    self._logger.exception('Fail to find %s | err -> %s', key, err)
                    raise StoreKeyNotFound
            else:
                raise BadEntryType
        else:
            raise UninitializedStore

    async def __set_in_store(self, key: str, value: bytes) -> None:
        """ Async private function for set key & value in persistency storage

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        await self._persistency.set(key, value)

    async def __delete_in_store(self, key: str) -> None:
        """ Async private function for del key & value in persistency storage

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        await self._persistency.delete(key)

    async def __ready(self, key: str) -> None:
        """ Check if entry is ready to get

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        try:
            if self._lock[key]:
                self._lock[key] = await self._lock_coordinator.get(key)
        except KeyError:
            raise StoreKeyNotFound

    async def __lock_entry(self, key: str) -> None:
        """ Lock entry by key

        Entry is locked while persistency writes data

        Args:
            key (str): Key entry for lock

        Returns:
            None
        """
        self._lock_coordinator.put_nowait(key, True)
        self._lock[key] = True

    def __unlock_entry(self, key: str, is_deleted: bool, task: Task) -> None:
        """ Unlock entry by key

        Entry is unlocked after persistency writes data

        Args:
            key (str): Key entry for lock
            task: (Task): Asyncio task

        Returns:
            None
        """
        self._lock_coordinator.put_nowait(key, False)
        self._lock[key] = False
        if is_deleted:
            del self._lock[key]

    async def _build_set(self, key: str, value: bytes) -> None:
        """ Set value & key in global store

        Args:
            key (str): Value key as string
            value (bytes): Value to store as bytes format

        Returns:
            None
        """
        if isinstance(key, str) and isinstance(value, bytes):
            await self._persistency.__getattribute__('_build_operations').__call__(key, value, StoreRecordType.SET)
            self._lock[key] = False
        else:
            raise BadEntryType

    async def _build_delete(self, key: str) -> None:
        """ Delete value by key in global store

        Args:
            key (str): Value key as string

        Returns:
            None
        """
        if isinstance(key, str):
            await self._persistency.__getattribute__('_build_operations').__call__(key, '', StoreRecordType.DEL)
            del self._lock[key]
        else:
            raise BadEntryType
