#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

"""Contain GlobalStore
"""

from logging import getLogger

from tonga.models.structs.persistency_type import PersistencyType
from tonga.stores.base import BaseStores
from tonga.stores.errors import BadEntryType
from tonga.stores.manager.errors import UninitializedStore
from tonga.stores.persistency.memory import MemoryPersistency
from tonga.stores.persistency.shelve import ShelvePersistency
from tonga.stores.persistency.rocksdb import RocksDBPersistency
from tonga.models.structs.store_record_type import StoreRecordType

__all__ = [
    'GlobalStore',
]


class GlobalStore(BaseStores):
    """ Global store
    """

    def __init__(self, db_type: PersistencyType, db_path: str = None):
        self._logger = getLogger('tonga')

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
        """ Get value by key in global store

        Args:
            key (str): Value key as string

        Returns:
            bytes: return value as bytes
        """

        if self._persistency.is_initialize():
            if isinstance(key, str):
                return await self._persistency.get(key)
            else:
                raise BadEntryType
        raise UninitializedStore

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
        else:
            raise BadEntryType
