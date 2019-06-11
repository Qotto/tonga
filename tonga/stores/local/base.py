#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, List

from aiokafka import TopicPartition

from tonga.stores import BaseStores, BaseStoreMetaData

__all__ = [
    'BaseLocalStore',
]


class BaseLocalStore(BaseStores):
    """ Base of all local stores
    """
    async def set_store_position(self, current_instance: int, nb_replica: int,
                                 assigned_partitions: List[TopicPartition],
                                 last_offsets: Dict[TopicPartition, int]) -> None:
        """ Set store position (consumer offset)

        Abstract method

        Args:
            current_instance (int): Project current instance
            nb_replica (int): Number of project replica
            assigned_partitions (List[TopicPartition]): List of assigned partition
            last_offsets (Dict[TopicPartition, int]): List of last offsets consumed by store

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    def is_initialized(self) -> bool:
        """ Return store state

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            bool: true if store is initialize, otherwise false
        """
        raise NotImplementedError

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

    async def get(self, key: str) -> bytes:
        """ Get value by key

        Abstract method

        Args:
            key (str): Value key as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            bytes: return value as bytes
        """
        raise NotImplementedError

    async def set(self, key: str, value: bytes) -> None:
        """ Set in store key & value

        Abstract method

        Args:
            key (str): Key of value as string
            value (bytes): value as bytes

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        """ Delete in store value by key

        Abstract method

        Args:
            key (str): Key of value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def build_set(self, key: str, value: bytes) -> None:
        """ Set key & value when store is not initialized

        Abstract method

        Args:
            key (str): Key of value as string
            value (bytes): Value as bytes

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def build_delete(self, key: str) -> None:
        """ Delete value by key when store is not initialized

        Abstract method

        Args:
            key (str): Key of value as string

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def get_all(self) -> Dict[str, bytes]:
        """ Get all value in store in dict format

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Dict[str, bytes]: return db copy as dict format
        """
        raise NotImplementedError

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

    async def update_metadata_tp_offset(self, tp: TopicPartition, offset: int) -> None:
        """ Update store metadata

        Abstract method

        Args:
            tp (TopicPartition): Kafka topic partition
            offset (int): Kafka offset

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def get_metadata(self) -> BaseStoreMetaData:
        """ Return store metadata class

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            BaseStoreMetaData: return positioning class
        """
        raise NotImplementedError

    async def _update_metadata(self) -> None:
        """ Store metadata in db

        Internal function / Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError

    async def flush(self) -> None:
        """ Flush store

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            None
        """
        raise NotImplementedError
