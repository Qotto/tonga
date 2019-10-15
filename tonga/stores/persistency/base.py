#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from logging import Logger
from abc import ABCMeta, abstractmethod

from tonga.models.structs.store_record_type import StoreRecordType

__all__ = [
    'BasePersistency'
]


class BasePersistency(metaclass=ABCMeta):
    _initialize: bool = False
    _logger: Logger

    def is_initialize(self) -> bool:
        """ Return true if persistency is initialized, false otherwise

        Returns:
            bool
        """
        return self._initialize

    def _set_initialize(self) -> None:
        """ Set persistency initialize flag to true

            Returns:
                None
        """
        self._initialize = True

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """ Get value by key in local store

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        raise NotImplementedError

    @abstractmethod
    async def set(self, key: str, value: bytes) -> None:
        """ Set value & key in global store

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key: str) -> None:
        """ Delete value by key in global store

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def _build_operations(self, key: str, value: bytes, operation_type: StoreRecordType) -> None:
        """ This function is used for build DB when store is not initialize

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes
            operation_type (StoreRecordType): Operation type (SET or DEL)

        Returns:
            None
        """
        raise NotImplementedError
