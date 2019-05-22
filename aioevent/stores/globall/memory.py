#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import ast
from aiokafka import TopicPartition

from typing import Dict, Any, List, Union

from aioevent.stores.globall.base import BaseGlobalStore
from aioevent.stores.base import BaseStoreMetaData
from aioevent.models.exceptions import StoreKeyNotFound, StoreMetadataCantNotUpdated
from aioevent.utils.decorator import check_initialized


class GlobalStoreMemory(BaseGlobalStore):
    _db: Dict[str, bytes]
    _store_metadata: Union[BaseStoreMetaData, None]
    _current_instance: int
    _nb_replica: int
    _event_sourcing: bool

    _assigned_partitions: List[TopicPartition]
    _last_offsets: Dict[TopicPartition, int]

    _initialized: bool

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # DB as dict in StoreMemory
        self._db = dict()

        # Default param
        self._current_instance = 0
        self._nb_replica = 1
        self._initialized = False
        self._store_metadata = None
        self._event_sourcing = False

        # Local store position
        self._assigned_partitions = list()
        self._last_offsets = dict()

    def set_store_position(self, current_instance: int, nb_replica: int, assigned_partitions: List[TopicPartition],
                           last_offsets: Dict[TopicPartition, int]) -> None:
        self._assigned_partitions = assigned_partitions
        self._last_offsets = last_offsets
        self._nb_replica = nb_replica
        self._current_instance = current_instance

        self._store_metadata = BaseStoreMetaData(self._assigned_partitions, self._last_offsets, self._current_instance,
                                                 self._nb_replica)
        self._update_metadata()

    def set_initialized(self, initialized: bool) -> None:
        self._initialized = initialized

    def is_initialized(self) -> bool:
        return self._initialized

    @check_initialized
    def get(self, key: str) -> Any:
        if not isinstance(key, str):
            raise ValueError
        if key not in self._db:
            raise StoreKeyNotFound(f'key: {key} was not found in GlobalStoreMemory', 500)
        return self._db[key]

    @check_initialized
    def get_all(self) -> Dict[str, Any]:
        return self._db.copy()

    def global_set(self, key: str, value: bytes) -> None:
        if not isinstance(key, str):
            raise ValueError
        if key == 'metadata':
            raise StoreMetadataCantNotUpdated(f'Fail to update metadata with set function', 500)
        self._db[key] = value

    def global_delete(self, key: str) -> None:
        if not isinstance(key, str):
            raise ValueError
        if key not in self._db:
            raise StoreKeyNotFound(f'Fail to delete key: {key}, not found in GlobalStoreMemory', 500)
        del self._db[key]

    def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        self._db['metadata'] = bytes(str(metadata.to_dict()), 'utf-8')

    def get_metadata(self) -> BaseStoreMetaData:
        return BaseStoreMetaData.from_dict(ast.literal_eval(self._db['metadata'].decode('utf-8')))

    def update_metadata_tp_offset(self, tp: TopicPartition, offset: int):
        self._store_metadata.update_last_offsets(tp, offset)
        self._update_metadata()

    def _update_metadata(self) -> None:
        self._db['metadata'] = bytes(str(self._store_metadata.to_dict()), 'utf-8')

    def flush(self) -> None:
        del self._db
        self._db = {'metadata': bytes(str(self._store_metadata.to_dict()), 'utf-8')}