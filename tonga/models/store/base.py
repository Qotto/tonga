#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseHandler Class

All handlers must be inherit form this class
"""

from tonga.models.store.store_record import StoreRecord
from tonga.models.structs.positioning import BasePositioning

__all__ = [
    'BaseStoreRecordHandler'
]


class BaseStoreRecordHandler:
    """ Base of all StoreRecordHandler
    """

    def __init__(self):
        """ BaseStoreRecordHandler constructor
        """

    @classmethod
    def handler_name(cls) -> str:
        """ Return store record handler name, used by serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

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
        raise NotImplementedError

    async def global_store_handler(self, store_record: StoreRecord,  positioning: BasePositioning) -> None:
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
        raise NotImplementedError
