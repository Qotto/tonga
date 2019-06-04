#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import uuid
from datetime import datetime as py_datetime
from datetime import timezone

from typing import Dict, Any

from tonga.utils.gen_correlation_id import gen_correlation_id

__all__ = [
    'BaseModel',
]


class BaseModel(object):
    name: str
    schema_version: str
    record_id: str
    partition_key: str
    timestamp: int
    datetime: str
    correlation_id: str
    context: Dict[str, Any]

    def __init__(self, record_id: str = None, schema_version: str = None, partition_key: str = None,
                 correlation_id: str = None, datetime: str = None, timestamp: int = None,
                 context: Dict[str, Any] = None) -> None:

        if record_id is None:
            self.record_id = uuid.uuid4().hex
        else:
            self.record_id = record_id

        if partition_key is None:
            self.partition_key = '0'
        else:
            self.partition_key = partition_key

        if schema_version is None:
            self.schema_version = '0.0.0'
        else:
            self.schema_version = schema_version

        if correlation_id is None:
            self.correlation_id = gen_correlation_id()
        else:
            self.correlation_id = correlation_id

        if timestamp is None:
            self.timestamp = round(py_datetime.now(timezone.utc).timestamp()*1000)
        else:
            self.timestamp = timestamp

        if datetime is None:
            self.datetime = py_datetime.now(timezone.utc).isoformat()
        else:
            self.datetime = datetime

        if context is None:
            self.context = dict()
        else:
            self.context = context

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError
