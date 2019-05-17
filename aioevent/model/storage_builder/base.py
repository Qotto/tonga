#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime as py_datetime
from datetime import timezone
from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.services.store_builder.store_builder import StoreBuilder

__all__ = [
    'BaseStorageBuilder'
]


class BaseStorageBuilder(object):
    name: str
    schema_version: str
    timestamp: int
    datetime: str

    def __init__(self, schema_version: str = None, datetime: str = None, timestamp: int = None) -> None:
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

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError

    async def local_state_handler(self, store_builder: StoreBuilder, group_id: str, topic: TopicPartition,
                                  offset: int) -> None:
        raise NotImplementedError

    async def global_state_handler(self, store_builder: StoreBuilder, group_id: str, topic: TopicPartition,
                                   offset: int) -> None:
        raise NotImplementedError
