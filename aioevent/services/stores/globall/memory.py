#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import ast
from aiokafka import TopicPartition

from typing import Dict, Any, List

from aioevent.services.stores.globall.base import BaseGlobalStore
from aioevent.services.stores import BaseStoreMetaData
from aioevent.model.exceptions import StoreKeyNotFound, StoreMetadataCantNotUpdated


class GlobalStoreMemory(BaseGlobalStore):
    _db = Dict[str, bytes]
    store_metadata = BaseStoreMetaData

    def __init__(self, assigned_partitions: List[TopicPartition], last_offsets: Dict[TopicPartition, int],
                 current_instance: int, nb_replica: int, **kwargs):
        super().__init__(**kwargs)
        self._db = dict()
        self.store_metadata = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)
        self._update_metadata()

    def get(self, key: str) -> Any:
        if not isinstance(key, str):
            raise ValueError
        if key not in self._db:
            raise StoreKeyNotFound(f'key: {key} was not found in GlobalStoreMemory', 500)
        return self._db[key]

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
        self.store_metadata.update_last_offsets(tp, offset)
        self._update_metadata()

    def _update_metadata(self) -> None:
        self._db['metadata'] = bytes(str(self.store_metadata.to_dict()), 'utf-8')
