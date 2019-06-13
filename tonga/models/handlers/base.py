#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseHandler Class

All handlers must be inherit form this class
"""

from aiokafka import TopicPartition

from tonga.models.records.base import BaseStoreRecord

__all__ = [
    'BaseHandler',
    'BaseStoreRecordHandler'
]


class BaseHandler:
    """ Base of all handler class
    """

    @classmethod
    def handler_name(cls) -> str:
        """ Return handler name, used by serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError


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
        raise NotImplementedError

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
        raise NotImplementedError
