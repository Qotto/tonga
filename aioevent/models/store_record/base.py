#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime as py_datetime
from datetime import timezone
from aiokafka import TopicPartition

from typing import Dict, Any

__all__ = [
    'BaseStoreRecord',
    'BaseStoreRecordHandler'
]


class BaseStoreRecord(object):
    name: str
    schema_version: str
    timestamp: int
    datetime: str
    key: str
    ctype: str
    value: bytes

    def __init__(self, key: str, ctype: str, value: bytes, schema_version: str = None,
                 datetime: str = None, timestamp: int = None) -> None:
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
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError


class BaseStoreRecordHandler:
    def __init__(self):
        pass

    @classmethod
    def handler_name(cls) -> str:
        raise NotImplementedError

    async def local_store_handler(self, store_record: BaseStoreRecord, group_id: str, tp: TopicPartition,
                                  offset: int) -> None:
        raise NotImplementedError

    async def global_store_handler(self, store_record: BaseStoreRecord, group_id: str, tp: TopicPartition,
                                   offset: int) -> None:
        raise NotImplementedError
