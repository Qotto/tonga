#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain BaseStores / BaseStoreMetaData

All store must be inherit form BaseStore

BaseStoreMetaData is used for store positioning (Assigned topics partitions / kafka offset), each store contain
an BaseStoreMetaData but this one was never send in Kafka. Used by store or developer purpose
"""

from logging import Logger

from abc import ABCMeta, abstractmethod

from tonga.stores.persistency.base import BasePersistency

__all__ = [
    'BaseStores',
]


class BaseStores(metaclass=ABCMeta):
    """ Base of local & global stores

    Attributes:
        _persistency (BasePersistency): Store persistency
        _logger (Logger): Store logger
    """
    _persistency: BasePersistency
    _logger: Logger

    @abstractmethod
    def get(self, key: str) -> bytes:
        """ Get value in store by key

        Abstract method

        Args:
            key (str): Key of value

        Returns:
            bytes: values as bytes
        """
        raise NotImplementedError

    def get_persistency(self) -> BasePersistency:
        """ Return persistency store

        Returns:
            BasePersistency: persistency store
        """
        return self._persistency

    @abstractmethod
    async def _build_set(self, key: str, value: bytes) -> None:
        """ Set value & key in global store

        Args:
            key (str): Value key as string
            value (bytes): Value to store as bytes format

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def _build_delete(self, key: str) -> None:
        """ Delete value by key in global store

        Args:
            key (str): Value key as string

        Returns:
            None
        """
        raise NotImplementedError