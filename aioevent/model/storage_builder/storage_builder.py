#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.services.store_builder.store_builder import StoreBuilder
from aioevent.model.storage_builder.base import BaseStorageBuilder
from aioevent.model.exceptions import KtableUnknownType

__all__ = [
    'StorageBuilder'
]


class StorageBuilder(BaseStorageBuilder):
    key: str
    type: str
    value: bytes

    def __init__(self, key: bytes, ttype: str, value: bytes, **kwargs) -> None:
        super().__init__(**kwargs)
        self.key = key.decode('utf-8')
        self.type = ttype
        self.value = value

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.storage.builder'

    async def local_state_handler(self, store_builder: StoreBuilder, group_id: str, tp: TopicPartition, offset: int):
        # TODO check if is metadata
        if self.type == 'set':
            store_builder.get_local_store().set(self.key, self.value)
        elif self.type == 'del':
            store_builder.get_local_store().delete(self.key)
        else:
            raise KtableUnknownType(f'Local state type: {self.type} is unknown', 500)
        store_builder.get_local_store().update_metadata_tp_offset(tp, offset)

    async def global_state_handler(self, store_builder: StoreBuilder, group_id: str, tp: TopicPartition, offset: int):
        # TODO check if is metadata
        if self.type == 'set':
            store_builder.get_global_store().global_set(self.key, self.value)
        elif self.type == 'del':
            store_builder.get_global_store().global_delete(self.key)
        else:
            raise KtableUnknownType(f'Global state type: {self.type} is unknown', 500)
        store_builder.get_local_store().update_metadata_tp_offset(tp, offset)
