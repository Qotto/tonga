#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime as py_datetime
from datetime import timezone
from typing import Dict, Any

from aiokafka import TopicPartition

__all__ = [
    'BaseStoreRecord',
    'BaseStoreRecordHandler'
]


class BaseStoreRecord:
    """ Base of all StoreRecord

    Attributes:
        schema_version (str): Includes the schema version of the record, it helps to keep applications compatible
                              with older records in the system
        timestamp (int): UNIX timestamp in milliseconds, which is easy to read for machines.
        datetime (string): ISO-8601-encoded string, which is human readable, therefore useful for debugging purposes.
        key (str): Should be a key determining to which partition your record will be assigned.
                   Records with the same *key* value are guaranteed to be written to the same partition and by used for
                   Kafka compaction. Use an UUID for store value
        ctype (str): Record type (possible value set/del)
        value (bytes): Record value as bytes format
    """
    schema_version: str
    timestamp: int
    datetime: str
    key: str
    ctype: str
    value: bytes

    def __init__(self, key: str, ctype: str, value: bytes, schema_version: str = None,
                 datetime: str = None, timestamp: int = None) -> None:
        """ BaseStoreRecord constructor

        Args:
            key (str): Should be a key determining to which partition your record will be assigned.
                       Records with the same *key* value are guaranteed to be written to the same partition and by used
                       for Kafka compaction. Use an UUID for store value
            ctype (str): Record type (possible value set/del)
            value (bytes): Record value as bytes format
            schema_version (str): Includes the schema version of the record, it helps to keep applications compatible
                                  with older records in the system
            datetime (str): ISO-8601-encoded string, which is human readable, therefore useful for debugging purposes.
            timestamp (int): UNIX timestamp in milliseconds, which is easy to read for machines.

        Returns:
            None
        """
        if schema_version is None:
            self.schema_version = '0.0.0'
        else:
            self.schema_version = schema_version

        if timestamp is None:
            self.timestamp = round(py_datetime.now(timezone.utc).timestamp() * 1000)
        else:
            self.timestamp = timestamp

        if datetime is None:
            self.datetime = py_datetime.now(timezone.utc).isoformat()
        else:
            self.datetime = datetime
        self.key = key
        self.ctype = ctype
        self.value = value

    @classmethod
    def event_name(cls) -> str:
        """ Return store record Class name, used by serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        """ Serialize dict to StoreRecord

        Args:
            event_data (Dict|str, Any]): Contains all StoreRecord Class attribute for return an instanced class

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
