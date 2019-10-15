#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from logging import getLogger

from typing import Dict

from tonga.stores.errors import StoreKeyNotFound
from tonga.stores.manager.errors import UninitializedStore
from tonga.stores.persistency.errors import UnknownOperationType
from tonga.stores.persistency.base import BasePersistency
from tonga.models.structs.store_record_type import StoreRecordType


__all__ = [
    'MemoryPersistency'
]


class MemoryPersistency(BasePersistency):
    _db: Dict[str, bytes]

    def __init__(self):
        self._db = dict()
        self._initialize = False
        self._logger = getLogger('tonga')

    async def get(self, key: str) -> bytes:
        """ Get value by key

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        if self._initialize:
            try:
                return self._db[key]
            except (ValueError, KeyError, AttributeError) as err:
                self._logger.exception('Fail to get %s | Err: %s', key, err)
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
            self._db[key] = value
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
                del self._db[key]
            except (ValueError, KeyError, AttributeError) as err:
                self._logger.exception('Fail to delete %s | Err: %s', key, err)
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
            self._db[key] = value
        elif operation_type == StoreRecordType.DEL:
            try:
                del self._db[key]
            except (ValueError, KeyError, AttributeError) as err:
                self._logger.exception('Fail build operation delete %s | Err: %s', key, err)
                raise StoreKeyNotFound
        else:
            raise UnknownOperationType
