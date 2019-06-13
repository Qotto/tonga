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
from typing import Dict, Any, List

from aiokafka import TopicPartition

from tonga.stores.errors import StorePartitionAlreadyAssigned, StorePartitionNotAssigned

__all__ = [
    'BaseStores',
    'BaseStoreMetaData',
]


class BaseStoreMetaData:
    """ Store positioning class

    Attributes:
        assigned_partitions (List[TopicPartition]): List of assigned partition
        current_instance (int): Project current instance
        nb_replica (int): Number of project replica
        last_offsets (Dict[TopicPartition, int]): List of last offsets consumed by store
    """
    assigned_partitions: List[TopicPartition]
    current_instance: int
    nb_replica: int
    last_offsets: Dict[TopicPartition, int]

    def __init__(self, assigned_partitions: List[TopicPartition], last_offsets: Dict[TopicPartition, int],
                 current_instance: int, nb_replica: int) -> None:
        """ BaseStoreMetaData constructor

        Args:
            assigned_partitions (List[TopicPartition]): List of assigned partition
            current_instance (int): Project current instance
            nb_replica (int): Number of project replica
            last_offsets (Dict[TopicPartition, int]): List of last offsets consumed by store

        Returns:
            None
        """
        self.assigned_partitions = assigned_partitions
        self.current_instance = current_instance
        self.nb_replica = nb_replica
        self.last_offsets = last_offsets

    def update_last_offsets(self, tp: TopicPartition, offset: int) -> None:
        """ Update last offsets by TopicPartition

        Args:
            tp (TopicPartition): Kafka TopicPartition
            offset (int): Kafka offset

        Raises:
            StorePartitionNotAssigned: raised when partition was not assigned

        Returns:
            None
        """
        if tp not in self.last_offsets:
            raise StorePartitionNotAssigned
        self.last_offsets[tp] = offset

    def assign_partition(self, tp: TopicPartition) -> None:
        """Assign partition by TopicPartition

        Args:
            tp (TopicPartition): Kafka TopicPartition

        Raises:
            StorePartitionAlreadyAssigned: raised when partition is already assigned

        Returns:
            None
        """
        if tp not in self.assigned_partitions:
            self.assigned_partitions.append(tp)
            self.last_offsets[tp] = 0
        else:
            raise StorePartitionAlreadyAssigned

    def unassign_partition(self, tp: TopicPartition) -> None:
        """Unassing partition by TopicPartition

        Args:
            tp (TopicPartition): Kafka TopicPartition

        Raises:
            StorePartitionNotAssigned: raised when partition was not assigned

        Returns:
            None
        """
        if tp in self.assigned_partitions:
            self.assigned_partitions.remove(tp)
            del self.last_offsets[tp]
        raise StorePartitionNotAssigned

    def to_dict(self) -> Dict[str, Any]:
        """ Return class as dict

        Returns:
            Dict[str, Any]: class as dict format
        """
        return {
            'assigned_partitions': [{'topic': tp.topic, 'partition': tp.partition} for tp in self.assigned_partitions],
            'current_instance': self.current_instance,
            'nb_replica': self.nb_replica,
            'last_offsets': [{'topic': tp.topic, 'partition': tp.partition, 'offsets': key} for tp, key in
                             self.last_offsets.items()]
        }

    @classmethod
    def from_dict(cls, meta_dict: Dict[str, Any]):
        """ Return class form dict

        Args:
            meta_dict (Dict[str, Any]): Metadata class as dict format

        Returns:
            BaseStoreMetaData: return instanced class
        """
        assigned_partitions = [TopicPartition(tp['topic'], tp['partition']) for tp in
                               meta_dict['assigned_partitions']]
        last_offsets = {}
        for tp in meta_dict['last_offsets']:
            last_offsets[TopicPartition(tp['topic'], tp['partition'])] = tp['offsets']
        return cls(assigned_partitions=assigned_partitions, last_offsets=last_offsets,
                   current_instance=meta_dict['current_instance'], nb_replica=meta_dict['nb_replica'])


class BaseStores:
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

    async def get_all(self) -> Dict[str, bytes]:
        """ Get all value in store in dict

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Dict[str, bytes]: return db copy in dict
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
