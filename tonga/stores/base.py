#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain BaseStores / BaseStoreMetaData

All store must be inherit form BaseStore

BaseStoreMetaData is used for store positioning (Assigned topics partitions / kafka offset), each store contain
an BaseStoreMetaData but this one was never send in Kafka. Used by store or developer purpose
"""

import logging
from logging import Logger
from abc import ABCMeta, abstractmethod

from typing import Dict, Type

from tonga.stores.metadata.base import BaseStoreMetaData
from tonga.models.structs.positioning import BasePositioning

__all__ = [
    'BaseStores',
]


class BaseStores(metaclass=ABCMeta):
    """ Base of all stores

    Attributes:
        _name (str): Store name
        _logger (Logger): Store logger
    """
    _name: str
    _logger: Logger

    def __init__(self, name: str) -> None:
        """ BaseStores constructor

        Args:
            name (str): BaseStores name

        Returns:
            None
        """
        self._name = name
        self._logger = logging.getLogger('tonga')

    @abstractmethod
    def set_metadata_class(self, store_metadata_class: Type[BaseStoreMetaData]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def set_store_position(self, store_metadata: BaseStoreMetaData) -> None:
        """ Set store position (consumer offset)

        Abstract method

        Args:
            store_metadata (BaseStoreMetaData): Store metadata

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    def is_initialized(self) -> bool:
        """ Return store state

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            bool: true if store is initialize, otherwise false
        """
        raise NotImplementedError

    @abstractmethod
    def set_initialized(self, initialized: bool) -> None:
        """Set store state

        Abstract method

        Args:
            initialized (bool): true for initialize store, otherwise false

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """ Get value by key

        Abstract method

        Args:
            key (str): Value key abstract param

        Raises:
            NotImplementedError: Abstract method

        Returns:
            bytes: return value as bytes
        """
        raise NotImplementedError

    @abstractmethod
    async def get_all(self) -> Dict[str, bytes]:
        """ Get all value in store in dict

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Dict[str, bytes]: return db copy in dict
        """
        raise NotImplementedError

    @abstractmethod
    async def update_metadata_tp_offset(self, positioning: BasePositioning) -> None:
        """ Update store metadata

        Args:
            positioning (BasePositioning): Contains topic name / current partition / current offset

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        """ Set store metadata

        Abstract method

        Args:
            metadata (BaseStoreMetaData): Set store metadata, used for store positioning

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None

        """
        raise NotImplementedError

    @abstractmethod
    async def get_metadata(self) -> BaseStoreMetaData:
        """ Return store metadata class

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            BaseStoreMetaData: return positioning class
        """
        raise NotImplementedError

    @abstractmethod
    async def _update_metadata(self) -> None:
        """ Store metadata in db

        Internal function / Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def flush(self) -> None:
        """ Flush store

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError
