#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import ast
from aiokafka import TopicPartition

from typing import Dict, Any, List, Union

from tonga.stores.globall.base import BaseGlobalStore
from tonga.stores.base import BaseStoreMetaData
from tonga.utils.decorator import check_initialized

# Import store exceptions
from tonga.services.coordinator.partitioner.errors import BadKeyType
from tonga.stores.errors import (StoreKeyNotFound, StoreMetadataCantNotUpdated)


class GlobalStoreMemory(BaseGlobalStore):
    """ Global store memory

    Fast DB, but no persistence, At each start, it will have to rebuild own global store before being available

    Attributes:
        _db (Dict[str, bytes]): Dict will contain all keys & values
        _store_metadata (Union[BaseStoreMetaData, None]): StoreMetaData contain where store positioning in Kafka
        _current_instance (int): Service current instance
        _nb_replica (int): Max service instance
        _assigned_partitions (List[TopicPartition]): List of assigned partition
        _last_offsets (Dict[TopicPartition, int]): Dict contain last offset of each assigned partitions
        _initialized (bool): Db flag (true if store is initialized, otherwise false)
    """
    _db: Dict[str, bytes]
    _store_metadata: Union[BaseStoreMetaData, None]
    _current_instance: int
    _nb_replica: int

    _assigned_partitions: List[TopicPartition]
    _last_offsets: Dict[TopicPartition, int]

    _initialized: bool

    def __init__(self, **kwargs) -> None:
        """GlobalStoreMemory constructor

        Args:
            **kwargs (Dict[str, Any]): Dict for BaseStore (param *name*)

        Returns:
            None
        """
        super().__init__(**kwargs)
        # DB as dict in StoreMemory
        self._db = dict()

        # Default param
        self._current_instance = 0
        self._nb_replica = 1
        self._initialized = False
        self._store_metadata = None

        # Local store position
        self._assigned_partitions = list()
        self._last_offsets = dict()

    async def set_store_position(self, current_instance: int, nb_replica: int,
                                 assigned_partitions: List[TopicPartition],
                                 last_offsets: Dict[TopicPartition, int]) -> None:
        """ Set store position (consumer offset)

        Args:
            current_instance (int): Project current instance
            nb_replica (int): Number of project replica
            assigned_partitions (List[TopicPartition]): List of assigned partition
            last_offsets (Dict[TopicPartition, int]): List of last offsets consumed by store

        Returns:
            None
        """
        self._assigned_partitions = assigned_partitions
        self._last_offsets = last_offsets
        self._nb_replica = nb_replica
        self._current_instance = current_instance

        self._store_metadata = BaseStoreMetaData(self._assigned_partitions, self._last_offsets, self._current_instance,
                                                 self._nb_replica)
        await self._update_metadata()

    def set_initialized(self, initialized: bool) -> None:
        """Set store state

        Args:
            initialized (bool): true for initialize store, otherwise false

        Returns:
            None
        """
        self._logger.info(f'GlobalStoreMemory initialized: {initialized}')
        self._initialized = initialized

    def is_initialized(self) -> bool:
        """ Return store state

        Returns:
            bool: true if store is initialize, otherwise false
        """
        return self._initialized

    @check_initialized
    async def get(self, key: str) -> Any:
        """ Get value by key

        Args:
            key (str): Value key as string

        Raises:
            BadKeyType: raised when key was not bytes
            StoreKeyNotFound: raised when store not found value by key

        Returns:
            bytes: return value as bytes
        """
        if not isinstance(key, str):
            raise BadKeyType
        if key not in self._db:
            raise StoreKeyNotFound
        return self._db[key]

    @check_initialized
    async def get_all(self) -> Dict[str, Any]:
        """ Get all value in store in dict

        Returns:
            Dict[str, bytes]: return db copy in dict
        """
        return self._db.copy()

    async def global_set(self, key: str, value: bytes) -> None:
        """ Set key & value in global store

        Args:
            key (str): Key value as string
            value (bytes): Values as bytes

        Raises:
            BadKeyType: raised when key was not bytes
            StoreMetadataCantNotUpdated: raised when store can't update StoreMetadata

        Returns:
            None
        """
        if not isinstance(key, str):
            raise BadKeyType
        if key == 'metadata':
            raise StoreMetadataCantNotUpdated
        self._db[key] = value

    async def global_delete(self, key: str) -> None:
        """ Delete value by key in global store

        Args:
            key (str): Key value as string

        Raises:
            BadKeyType: raised when key was not bytes
            StoreKeyNotFound: raised when store not found value by key

        Returns:
            None
        """
        if not isinstance(key, str):
            raise BadKeyType
        if key not in self._db:
            raise StoreKeyNotFound
        del self._db[key]

    async def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        """ Set store metadata

        Args:
            metadata (BaseStoreMetaData): Set store metadata, used for store positioning

        Returns:
            None

        """
        self._db['metadata'] = bytes(str(metadata.to_dict()), 'utf-8')

    async def get_metadata(self) -> BaseStoreMetaData:
        """ Return store metadata class

        Returns:
            BaseStoreMetaData: return positioning class
        """
        return BaseStoreMetaData.from_dict(ast.literal_eval(self._db['metadata'].decode('utf-8')))

    async def update_metadata_tp_offset(self, tp: TopicPartition, offset: int):
        """ Update store metadata

        Args:
            tp (TopicPartition): Kafka topic partition
            offset (int): Kafka offset

        Returns:
            None
        """
        self._store_metadata.update_last_offsets(tp, offset)
        await self._update_metadata()

    async def _update_metadata(self) -> None:
        """ Store metadata in db

        Internal function

        Returns:
            None
        """
        self._db['metadata'] = bytes(str(self._store_metadata.to_dict()), 'utf-8')

    async def flush(self) -> None:
        """ Flush store

        Returns:
            None
        """
        del self._db
        self._db = {'metadata': bytes(str(self._store_metadata.to_dict()), 'utf-8')}
