#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from logging import getLogger

import pyrocksdb

from tonga.stores.persistency.base import BasePersistency
from tonga.models.structs.store_record_type import StoreRecordType
from tonga.stores.errors import StoreKeyNotFound
from tonga.stores.manager.errors import UninitializedStore
from tonga.stores.persistency.errors import UnknownOperationType, RocksDBErrors

__all__ = [
    'RocksDBPersistency'
]


class RocksDBPersistency(BasePersistency):
    _db: pyrocksdb.DB
    _opts: pyrocksdb.Options
    _wopts: pyrocksdb.WriteOptions
    _ropts: pyrocksdb.ReadOptions

    def __init__(self, db_name: str):
        self._logger = getLogger('tonga')
        self._db = pyrocksdb.DB()

        self._opts = pyrocksdb.Options()

        self._opts.create_if_missing = True

        s: pyrocksdb.Status = self._db.open(self._opts, db_name)

        if not s.ok():
            self._logger.error('RockDB open fail -> %s', s.to_string())
            raise RocksDBErrors

        self._wopts = pyrocksdb.WriteOptions()
        self._ropts = pyrocksdb.ReadOptions()

        self._initialize = False

    async def get(self, key: str) -> bytes:
        """ Get value by key

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        if self._initialize:
            try:
                print(key)
                blob: pyrocksdb.Blob = self._db.get(self._ropts, key.encode('utf-8'))
                if not blob.status.ok():
                    self._logger.error('Fail to get %s, info -> %s', key, blob.status.to_string())
                    raise StoreKeyNotFound
                return blob.data
            except (ValueError, KeyError, AttributeError):
                raise StoreKeyNotFound
        raise UninitializedStore

    async def set(self, key: str, value: bytes) -> None:
        """ Set value & key

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        if self._initialize:
            s = self._db.put(self._wopts, key.encode('utf-8'), value)
            if not s.ok():
                self._logger.error('Fail to set %s, info -> %s', key, s.to_string())
                raise RocksDBErrors
        else:
            raise UninitializedStore

    async def delete(self, key: str) -> None:
        """ Delete value by key

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        if self._initialize:
            try:
                s = self._db.delete(self._wopts, key.encode('utf-8'))
                print(s.to_string())
                if not s.ok():
                    self._logger.error('Fail to delete %s, info -> %s', key, s.to_string())
                    raise RocksDBErrors
            except (ValueError, KeyError, AttributeError):
                raise StoreKeyNotFound
        else:
            raise UninitializedStore

    async def _build_operations(self, key: str, value: bytes, operation_type: StoreRecordType) -> None:
        """ This function is used for build DB when store is not initialize

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes
            operation_type (StoreRecordType): Operation type (SET or DEL)

        Returns:
            None
        """
        if operation_type == StoreRecordType.SET:
            s = self._db.put(self._wopts, key.encode('utf-8'), value)
            if not s.ok():
                self._logger.error('Fail build operations set %s, info -> %s', key, s.to_string())
                raise RocksDBErrors
        elif operation_type == StoreRecordType.DEL:
            s = self._db.delete(self._wopts, key.encode('utf-8'))
            if not s.ok():
                self._logger.error('Fail build operations delete %s, info -> %s', key, s.to_string())
                raise RocksDBErrors
        else:
            raise UnknownOperationType

    def __del__(self):
        self._logger.info('Closed RocksDB')
        self._db.close()
