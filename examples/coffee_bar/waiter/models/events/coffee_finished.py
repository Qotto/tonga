#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone

from typing import Dict, Any

from tonga.models.records.event import BaseEvent

__all__ = [
    'CoffeeFinished'
]


class CoffeeFinished(BaseEvent):
    uuid: str
    served_to: str
    coffee_time: int

    def __init__(self, uuid: str, coffee_time: int, coffee_for: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.coffee_time = coffee_time
        self.coffee_for = coffee_for

    def to_dict(self) -> Dict[str, Any]:
        r_dict = self.base_dict()
        r_dict['uuid'] = self.uuid
        r_dict['coffee_for'] = self.coffee_for
        r_dict['coffee_time'] = self.coffee_time
        return r_dict

    @classmethod
    def from_dict(cls, dict_data: Dict[str, Any]):
        return cls(schema_version=dict_data['schema_version'],
                   record_id=dict_data['record_id'],
                   partition_key=dict_data['partition_key'],
                   date=datetime.fromtimestamp(dict_data['timestamp'] / 1000, timezone.utc),
                   correlation_id=dict_data['correlation_id'],
                   context=dict_data['context'],
                   uuid=dict_data['uuid'],
                   coffee_for=dict_data['coffee_for'],
                   coffee_time=dict_data['coffee_time'])

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.bartender.event.CoffeeFinished'
