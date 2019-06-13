#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseStoreBuilder

All store builder must be inherit form this class
"""

from aiokafka import TopicPartition

from tonga.services.consumer.base import BaseConsumer
from tonga.services.producer.base import BaseProducer
from tonga.stores.local.base import BaseLocalStore
from tonga.stores.globall.base import BaseGlobalStore

__all__ = [
    'BaseStoreBuilder'
]


class BaseStoreBuilder:
    """ Base store builder, all store builder must be inherit form this class
    """

    async def initialize_store_builder(self) -> None:
        """ Initialize store builder, this function was call by consumer for init store builder and set StoreMetaData
        Seek to last committed offset if store_metadata exist.

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    # Get stores
    def get_local_store(self) -> BaseLocalStore:
        """ Return local store instance

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            BaseLocalStore: local store instance
        """
        raise NotImplementedError

    def get_global_store(self) -> BaseGlobalStore:
        """ Return global store instance

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            BaseGlobalStore: global store instance
        """
        raise NotImplementedError

    # Get info
    def get_current_instance(self) -> int:
        """ Return service current instance

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            int: service current instance
        """
        raise NotImplementedError

    def is_event_sourcing(self) -> bool:
        """ Return event sourcing value

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            int: true if event sourcing is enable, otherwise false
        """
        raise NotImplementedError

    # Get producer & consumer
    def get_producer(self) -> BaseProducer:
        """ Return producer instance

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            BaseProducer: current producer instance
        """
        raise NotImplementedError

    def get_consumer(self) -> BaseConsumer:
        """ Return consumer instance

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            BaseConsumer: current consumer instance
        """
        raise NotImplementedError

    # Functions for local store management
    async def set_from_local_store(self, key: str, value: bytes) -> None:
        """ Set key & value in local store

        Abstract method

        Args:
            key (str): Key value as string
            value (bytes): Value as bytes

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def get_from_local_store(self, key: str) -> bytes:
        """ Get value by key in local store

        Abstract method

        Args:
            key: Key value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            bytes: value as bytes
        """
        raise NotImplementedError

    async def delete_from_local_store(self, key: str) -> None:
        """ Delete value from local store by key

        Abstract method

        Args:
            key (str): Key value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def update_metadata_from_local_store(self, tp: TopicPartition, offset: int) -> None:
        """ Update local store metadata

        Abstract method

        Args:
            tp (TopicPartition): Topic / Partition (see kafka-python for more details)
            offset (int): Current offset

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def set_from_local_store_rebuild(self, key: str, value: bytes) -> None:
        """ Set key & value in local store in rebuild mod

        Abstract method

        Args:
            key (str): Key value as string
            value (bytes): Value as bytes

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def delete_from_local_store_rebuild(self, key: str) -> None:
        """ Delete value by key in local store in rebuild mod

        Abstract method

        Args:
            key (str): Key value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    # Function for global store management
    async def get_from_global_store(self, key: str) -> bytes:
        """ Get value by key in global store

        Abstract method

        Args:
            key (str): Key value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            bytes: return value as bytes
        """
        raise NotImplementedError

    async def set_from_global_store(self, key: str, value: bytes) -> None:
        """ Set key & value in global store

        Abstract method

        Warnings:
            DO NOT USE THIS FUNCTION, IT ONLY CALLED BY CONSUMER FOR GLOBAL STORE CONSTRUCTION

        Args:
            key (str): Key value as string
            value (bytes): Value as bytes

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def delete_from_global_store(self, key: str) -> None:
        """ Delete key & value in global store

        Abstract method

        Warnings:
            DO NOT USE THIS FUNCTION, IT ONLY CALLED BY CONSUMER FOR GLOBAL STORE CONSTRUCTION

        Args:
            key (str): Key value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def update_metadata_from_global_store(self, tp: TopicPartition, offset: int) -> None:
        """ Update global store metadata

        Abstract method

        Args:
            tp (TopicPartition): Topic / Partition (see kafka-python for more details)
            offset (int): Current offset

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError
