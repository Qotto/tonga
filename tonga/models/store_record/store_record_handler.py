#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from tonga.models.store_record.base import BaseStoreRecordHandler, BaseStoreRecord
from tonga.stores.store_builder.base import BaseStoreBuilder

# Import StoreRecordHandler exceptions
from tonga.models.store_record.errors import (UnknownStoreRecordType)

__all__ = [
    'StoreRecordHandler'
]


class StoreRecordHandler(BaseStoreRecordHandler):
    """ StoreRecordHandler Class

    Attributes:
        _store_builder (BaseStoreBuilder): Store builder class, used for build & maintain local & global store
    """
    _store_builder: BaseStoreBuilder

    def __init__(self, store_builder: BaseStoreBuilder) -> None:
        """ StoreRecordHandler constructor

        Args:
            store_builder (BaseStoreBuilder): Store builder class, used for build & maintain local & global store

        Returns:
            None
        """
        super().__init__()
        self._store_builder = store_builder

    @classmethod
    def handler_name(cls) -> str:
        """ Return store record handler name, used by serializer

        Returns:
            str: StoreRecordHandler name
        """
        return 'tonga.store.record'

    async def local_store_handler(self, store_record: BaseStoreRecord, group_id: str, tp: TopicPartition,
                                  offset: int) -> None:
        """ This function is automatically call by Tonga when an BaseStore with same name was receive by consumer.
        Used for build local store.

        Args:
            store_record (BaseStoreRecord): StoreRecord event receive by consumer
            tp (TopicPartition): NamedTuple with topic name & partition number (more information in kafka-python
                                 or aiokafka
            group_id (str): Consumer group id, useful for make transaction in handler
            offset (int): Offset of receive message (used for commit transaction)

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """

        # Set or delete from local store
        if store_record.ctype == 'set':
            await self._store_builder.set_from_local_store_rebuild(store_record.key, store_record.value)
        elif store_record.ctype == 'del':
            await self._store_builder.delete_from_local_store_rebuild(store_record.key)
        else:
            raise UnknownStoreRecordType
        # Update metadata from local store
        await self._store_builder.update_metadata_from_local_store(tp, offset)

    async def global_store_handler(self, store_record: BaseStoreRecord, group_id: str, tp: TopicPartition,
                                   offset: int) -> None:
        """ This function is automatically call by Tonga when an BaseStore with same name was receive by consumer.
        Used for build global store.

        Args:
            store_record (BaseStoreRecord): StoreRecord event receive by consumer
            tp (TopicPartition): NamedTuple with topic name & partition number (more information in kafka-python
                                 or aiokafka
            group_id (str): Consumer group id, useful for make transaction in handler
            offset (int): Offset of receive message (used for commit transaction)

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """

        # Set or delete from global store
        if store_record.ctype == 'set':
            await self._store_builder.set_from_global_store(store_record.key, store_record.value)
        elif store_record.ctype == 'del':
            await self._store_builder.delete_from_global_store(store_record.key)
        else:
            raise UnknownStoreRecordType
        # Update metadata from global store
        await self._store_builder.update_metadata_from_global_store(tp, offset)
