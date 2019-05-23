#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from aioevent.models.store_record.base import BaseStoreRecordHandler, BaseStoreRecord
from aioevent.stores.store_builder.base import BaseStoreBuilder
from aioevent.models.exceptions import KtableUnknownType

__all__ = [
    'StoreRecordHandler'
]


class StoreRecordHandler(BaseStoreRecordHandler):
    _store_builder: BaseStoreBuilder

    def __init__(self, store_builder):
        super().__init__()
        self._store_builder = store_builder

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.store.record'

    async def local_store_handler(self, store_record: BaseStoreRecord, group_id: str, tp: TopicPartition,
                                  offset: int) -> None:
        # Set or delete from local store
        if store_record.type == 'set':
            await self._store_builder.set_from_local_store_rebuild(store_record.key, store_record.value)
        elif store_record.type == 'del':
            await self._store_builder.delete_from_local_store_rebuild(store_record.key)
        else:
            raise KtableUnknownType(f'Local state type: {store_record.type} is unknown', 500)
        # Update metadata from local store
        await self._store_builder.update_metadata_from_local_store(tp, offset)

    async def global_store_handler(self, store_record: BaseStoreRecord, group_id: str, tp: TopicPartition,
                                   offset: int) -> None:
        # Set or delete from global store
        if store_record.type == 'set':
            await self._store_builder.set_from_global_store(store_record.key, store_record.value)
        elif store_record.type == 'del':
            await self._store_builder.delete_from_global_store(store_record.key)
        else:
            raise KtableUnknownType(f'Global state type: {store_record.type} is unknown', 500)
        # Update metadata from global store
        await self._store_builder.update_metadata_from_global_store(tp, offset)
