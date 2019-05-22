#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.stores.store_builder.base import BaseStoreBuilder
from aioevent.models.storage_builder.base import BaseStorageBuilder
from aioevent.models.exceptions import KtableUnknownType

__all__ = [
    'StorageBuilder'
]


class StorageBuilder(BaseStorageBuilder):
    key: str
    type: str
    value: bytes

    def __init__(self, key: str, ttype: str, value: bytes, **kwargs) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.type = ttype
        self.value = value

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.storage.builder'

    async def local_state_handler(self, store_builder: BaseStoreBuilder, group_id: str, tp: TopicPartition,
                                  offset: int) -> None:
        # Set or delete from local store
        if self.type == 'set':
            await store_builder.set_from_local_store_rebuild(self.key, self.value)
        elif self.type == 'del':
            await store_builder.delete_from_local_store_rebuild(self.key)
        else:
            raise KtableUnknownType(f'Local state type: {self.type} is unknown', 500)
        # Update metadata from local store
        store_builder.update_metadata_from_local_store(tp, offset)

    async def global_state_handler(self, store_builder: BaseStoreBuilder, group_id: str, tp: TopicPartition,
                                   offset: int) -> None:
        # Set or delete from global store
        if self.type == 'set':
            store_builder.set_from_global_store(self.key, self.value)
        elif self.type == 'del':
            store_builder.delete_from_global_store(self.key)
        else:
            raise KtableUnknownType(f'Global state type: {self.type} is unknown', 500)
        # Update metadata from global store
        store_builder.update_metadata_from_global_store(tp, offset)
