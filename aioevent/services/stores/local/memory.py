#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import ast
from aiokafka import TopicPartition

from typing import Dict, Any, List

from aioevent.services.stores.local.base import BaseLocalStore
from aioevent.services.stores import BaseStoreMetaData
from aioevent.model.exceptions import StoreKeyNotFound, StoreMetadataCantNotUpdated


class LocalStoreMemory(BaseLocalStore):
    _db: Dict[str, bytes]
    store_metadata: BaseStoreMetaData

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
            raise StoreKeyNotFound(f'key: {key} was not found in LocalStoreMemory', 500)
        return self._db[key]

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(key, str) or not isinstance(value, bytes):
            raise ValueError
        if key == 'metadata':
            raise StoreMetadataCantNotUpdated(f'Fail to update metadata with set function', 500)
        self._db[key] = value

    def delete(self, key: str) -> None:
        if not isinstance(key, str):
            raise ValueError
        if key not in self._db:
            raise StoreKeyNotFound(f'Fail to delete key: {key}, not found in LocalStoreMemory', 500)
        del self._db[key]

    def get_all(self) -> Dict[str, bytes]:
        return self._db

    def update_metadata_tp_offset(self, tp: TopicPartition, offset: int) -> None:
        self.store_metadata.update_last_offsets(tp, offset)
        self._update_metadata()

    def get_metadata(self) -> BaseStoreMetaData:
        return BaseStoreMetaData.from_dict(ast.literal_eval(self._db['metadata'].decode('utf-8')))

    def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        self._db['metadata'] = bytes(str(metadata.to_dict()), 'utf-8')

    def _update_metadata(self) -> None:
        self._db['metadata'] = bytes(str(self.store_metadata.to_dict()), 'utf-8')
