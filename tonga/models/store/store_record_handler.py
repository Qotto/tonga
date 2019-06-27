#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" StoreRecordHandler class

This class was call when store consumer receive an new StoreRecord event and store msg in local & global store
"""

from tonga.models.store.base import BaseStoreRecordHandler
from tonga.models.store.store_record import StoreRecord
from tonga.stores.manager.base import BaseStoreManager

from tonga.models.structs.store_record_type import StoreRecordType
from tonga.models.structs.positioning import BasePositioning

# Import StoreRecordHandler exceptions
from tonga.models.store.errors import (UnknownStoreRecordType)

__all__ = [
    'StoreRecordHandler'
]


class StoreRecordHandler(BaseStoreRecordHandler):
    """ StoreRecordHandler Class

    Attributes:
        _store_builder (BaseStoreBuilder): Store builder class, used for build & maintain local & global store
    """
    _store_builder: BaseStoreManager

    def __init__(self, store_builder: BaseStoreManager) -> None:
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

    async def local_store_handler(self, store_record: StoreRecord, positioning: BasePositioning) -> None:
        """ This function is automatically call by Tonga when an BaseStore with same name was receive by consumer.
        Used for build local store.

        Args:
            store_record (BaseStoreRecord): StoreRecord event receive by consumer
            positioning (BasePositioning): Contains topic / partition / offset

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """

        # Set or delete from local store
        if store_record.operation_type == StoreRecordType('set'):
            await self._store_builder.set_from_local_store_rebuild(store_record.key, store_record.value)
        elif store_record.operation_type == StoreRecordType('del'):
            await self._store_builder.delete_from_local_store_rebuild(store_record.key)
        else:
            raise UnknownStoreRecordType
        # Update metadata from local store
        positioning.set_current_offset(positioning.get_current_offset() + 1)
        await self._store_builder.update_metadata_from_local_store(positioning)

    async def global_store_handler(self, store_record: StoreRecord, positioning: BasePositioning) -> None:
        """ This function is automatically call by Tonga when an BaseStore with same name was receive by consumer.
        Used for build global store.

        Args:
            store_record (BaseStoreRecord): StoreRecord event receive by consumer
            positioning (BasePositioning): Contains topic / partition / offset

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """

        # Set or delete from global store
        if store_record.operation_type == StoreRecordType('set'):
            await self._store_builder.set_from_global_store(store_record.key, store_record.value)
        elif store_record.operation_type == StoreRecordType('del'):
            await self._store_builder.delete_from_global_store(store_record.key)
        else:
            raise UnknownStoreRecordType
        # Update metadata from global store
        positioning.set_current_offset(positioning.get_current_offset() + 1)
        await self._store_builder.update_metadata_from_global_store(positioning)
