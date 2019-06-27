#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseStoreManager

All store manager must be inherit form this class
"""
from asyncio import Future

from tonga.models.structs.positioning import BasePositioning
from tonga.services.consumer.base import BaseConsumer
from tonga.services.producer.base import BaseProducer
from tonga.stores.global_store.base import BaseGlobalStore
from tonga.stores.local_store.base import BaseLocalStore

__all__ = [
    'BaseStoreManager'
]


class BaseStoreManager:
    """ Base store manager, all store manager must be inherit form this class
    """

    async def initialize_store_manager(self) -> None:
        """ Initialize store manager, this function was call by consumer for init store manager and set StoreMetaData
        Seek to last committed offset if store_metadata exist.

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    def return_consumer_task(self) -> Future:
        """ Return consumer future

        Returns:
            Future: return consumer future task
        """
        raise NotImplementedError

    def set_local_store_initialize(self, initialized: bool) -> None:
        """Set local store initialized flag

        Args:
            initialized (bool): true for set local store initialized, otherwise false

        Returns:
            None
        """
        raise NotImplementedError

    def set_global_store_initialize(self, initialized: bool) -> None:
        """Set global store initialized flag

        Args:
            initialized (bool): true for set global store initialized, otherwise false

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
            Abstract method
        """
        raise NotImplementedError

    async def get_from_local_store(self, key: str) -> bytes:
        """ Get value by key in local store

        Abstract method

        Args:
            key (str): Abstract param

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Abstract method
        """
        raise NotImplementedError

    async def delete_from_local_store(self, key: str) -> None:
        """ Delete value from local store by key

        Abstract method

        Args:
            key (key): Abstract parameter

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def update_metadata_from_local_store(self, positioning: BasePositioning) -> None:
        """ Update local store metadata

        Abstract method

        Args:
            positioning (BasePositioning) : Positioning class / contain topic / partition / offset

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
            key (str): Abstract parameter
            value (bytes): Abstract parameter

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
            key (str): Abstract parameter

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
            key (str): Abstract parameter

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
            key (str): Key of value as string
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
            key (str): Key of value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def update_metadata_from_global_store(self, positioning: BasePositioning) -> None:
        """ Update global store metadata

        Abstract method

        Args:
            positioning (BasePositioning) : Positioning class / contain topic / partition / offset

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError
