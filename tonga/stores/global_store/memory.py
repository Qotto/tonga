#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" GlobalStoreMemory

Global Store in memory save key / value in a dict. Fast DB, but no persistence, At each start, it will have to rebuild
own global store before being available
"""

import ast
from typing import Dict, Any, List, Union, Type

from tonga.models.structs.positioning import BasePositioning
from tonga.services.coordinator.partitioner.errors import BadKeyType
from tonga.stores.errors import (StoreKeyNotFound, StoreMetadataCantNotUpdated)
from tonga.stores.global_store.base import BaseGlobalStore
from tonga.stores.metadata.base import BaseStoreMetaData
from tonga.utils.decorator import check_initialized


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

    _assigned_partitions: List[BasePositioning]
    _last_offsets: Dict[str, BasePositioning]

    _initialized: bool
    _store_metadata_class: Type[BaseStoreMetaData]

    def __init__(self, **kwargs) -> None:
        """GlobalStoreMemory constructor

        Args:
            **kwargs: Dict for BaseStore (param *name*)

        Returns:
            None
        """
        super().__init__(**kwargs)
        # DB as dict in StoreMemory
        self._db = dict()

        # Default param
        self._nb_replica = 1
        self._current_instance = 0
        self._initialized = False
        self._store_metadata = None

        # Local store position
        self._assigned_partitions = list()
        self._last_offsets = dict()

    def set_metadata_class(self, store_metadata_class: Type[BaseStoreMetaData]):
        self._store_metadata_class = store_metadata_class

    async def set_store_position(self, store_metadata: BaseStoreMetaData) -> None:
        """ Set store position (consumer offset)

        Args:
            store_metadata (BaseStoreMetaData): Store metadata

        Returns:
            None
        """
        self._assigned_partitions = store_metadata.assigned_partitions
        self._last_offsets = store_metadata.last_offsets
        self._nb_replica = store_metadata.nb_replica
        self._current_instance = store_metadata.current_instance

        self._store_metadata = store_metadata

        await self._update_metadata()

    def set_initialized(self, initialized: bool) -> None:
        """Set store state

        Args:
            initialized (bool): true for initialize store, otherwise false

        Returns:
            None
        """
        self._logger.info('GlobalStoreMemory initialized: %s', initialized)
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

    async def update_metadata_tp_offset(self, positioning: BasePositioning) -> None:
        """ Update store metadata

        Args:
            positioning (BasePositioning): Contains topic name / current partition / current offset

        Returns:
            None
        """
        self._store_metadata.update_last_offsets(positioning)
        await self._update_metadata()

    async def get_metadata(self) -> BaseStoreMetaData:
        """ Return store metadata class

        Returns:
            BaseStoreMetaData: return positioning class
        """
        if 'metadata' not in self._db:
            raise StoreKeyNotFound
        return self._store_metadata_class.from_dict(ast.literal_eval(self._db['metadata'].decode('utf-8')))

    async def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        """ Set store metadata

        Args:
            metadata (BaseStoreMetaData): Set store metadata, used for store positioning

        Returns:
            None

        """
        self._db['metadata'] = bytes(str(metadata.to_dict()), 'utf-8')

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
