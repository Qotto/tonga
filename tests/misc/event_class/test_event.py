#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone

from tonga.models.records.event import BaseEvent

from typing import Dict, Any

__all__ = [
    'TestEvent'
]


class TestEvent(BaseEvent):
    test: str

    def __init__(self, test: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.test = test

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.test.event'

    def to_dict(self) -> Dict[str, Any]:
        r_dict = self.base_dict()
        r_dict['test'] = self.test
        return r_dict

    @classmethod
    def from_dict(cls, dict_data: Dict[str, Any]):
        return cls(schema_version=dict_data['schema_version'],
                   record_id=dict_data['record_id'],
                   partition_key=dict_data['partition_key'],
                   date=datetime.fromtimestamp(dict_data['timestamp'] / 1000, timezone.utc),
                   correlation_id=dict_data['correlation_id'],
                   context=dict_data['context'],
                   test=dict_data['test'])
