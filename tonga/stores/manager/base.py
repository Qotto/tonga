#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseStoreManager

All store manager must be inherit form this class
"""
from asyncio import AbstractEventLoop
from logging import Logger
from abc import ABCMeta, abstractmethod

from tonga.services.consumer.base import BaseConsumer
from tonga.services.producer.base import BaseProducer
from tonga.services.coordinator.client.base import BaseClient
from tonga.services.serializer.base import BaseSerializer
from tonga.stores.local_store import LocalStore
from tonga.stores.global_store import GlobalStore
from tonga.models.structs.persistency_type import PersistencyType

__all__ = [
    'BaseStoreManager'
]


class BaseStoreManager(metaclass=ABCMeta):
    """ Base store manager, all store manager must be inherit form this class
    """
    _local_store: LocalStore
    _global_store: GlobalStore
    _client: BaseClient
    _serializer: BaseSerializer
    _store_consumer: BaseConsumer
    _store_producer: BaseProducer
    _loop: AbstractEventLoop
    _rebuild: bool
    _persistency_type: PersistencyType
    _logger: Logger

    @abstractmethod
    async def _initialize_stores(self) -> None:
        """ This method initialize stores (construct, pre-build)

        Abstract method

        Returns:
            NOne
        """
        raise NotImplementedError

    def _initialize_local_store(self) -> None:
        """ This protected method set local store initialize flag to true

        Returns:
            None
        """
        self._local_store.get_persistency().__getattribute__('_set_initialize').__call__()

    def _initialize_global_store(self) -> None:
        """ This protected method set global store initialize flag to true

        Returns:
            None
        """
        self._global_store.get_persistency().__getattribute__('_set_initialize').__call__()

    def get_local_store(self) -> LocalStore:
        return self._local_store

    def get_global_store(self) -> GlobalStore:
        return self._global_store

    # Store function
    @abstractmethod
    async def set_entry_in_local_store(self, key: str, value: bytes) -> None:
        """ Set an entry in local store

        This method send an StoreRecord in event bus and store entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def get_entry_in_local_store(self, key: str) -> bytes:
        """ Get an entry by key in local store

        This method try to get an entry in local store asynchronously

        Abstract method

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_entry_in_local(self, key: str) -> None:
        """ Delete an entry in local store

        This method send an StoreRecord in event bus and delete entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def get_entry_in_global_store(self, key: str) -> bytes:
        """ Get an entry by key in global store

        This method try to get an entry in global store asynchronously

        Abstract method

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        raise NotImplementedError

    # Storage builder part
    @abstractmethod
    async def _build_set_entry_in_global_store(self, key: str, value: bytes) -> None:
        """ Set an entry in global store

        This protected method store an entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def _build_delete_entry_in_global_store(self, key: str) -> None:
        """ Delete an entry in global store

        This method delete an entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def _build_set_entry_in_local_store(self, key: str, value: bytes) -> None:
        """ Set an entry in local store

        This protected method store an entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def _build_delete_entry_in_local_store(self, key: str) -> None:
        """ Delete an entry in local store

        This method delete an entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        raise NotImplementedError
