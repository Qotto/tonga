#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any, List

from aioevent.services.stores.globall.base import BaseGlobalStore
from aioevent.services.stores import BaseStoreMetaData
from aioevent.model.exceptions import StoreKeyNotFound, StoreMetadataCantNotUpdated


class GlobalStoreMemory(BaseGlobalStore):
    _db = Dict[bytes, Any]
    store_metadata = BaseStoreMetaData

    def __init__(self, assigned_partitions: List[TopicPartition], last_offsets: Dict[TopicPartition, int],
                 current_instance: int, nb_replica: int):
        super().__init__()
        self._db = dict()
        self.store_metadata = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)
        self._update_metadata()

    def get(self, key: bytes) -> Any:
        if not isinstance(key, bytes):
            raise ValueError
        if key not in self._db:
            raise StoreKeyNotFound(f'key: {key} was not found in GlobalStoreMemory', 500)
        return self._db[key]

    def get_all(self) -> Dict[bytes, Any]:
        return self._db.copy()

    def update_metadata_tp_offset(self, tp: TopicPartition, offset: int):
        self.store_metadata.update_last_offsets(tp, offset)
        self._update_metadata()

    def global_set(self, key: bytes, value: Any) -> None:
        if not isinstance(key, bytes):
            raise ValueError
        if key == b'metadata':
            raise StoreMetadataCantNotUpdated(f'Fail to update metadata with set function', 500)
        self._db[key] = value

    def global_delete(self, key: bytes) -> None:
        if not isinstance(key, bytes):
            raise ValueError
        if key not in self._db:
            raise StoreKeyNotFound(f'Fail to delete key: {key}, not found in GlobalStoreMemory', 500)
        del self._db[key]

    def _get_metadata(self) -> BaseStoreMetaData:
        return BaseStoreMetaData.from_dict(self._db[b'metadata'])

    def _update_metadata(self) -> None:
        self._db[b'metadata'] = self.store_metadata.__dict__
