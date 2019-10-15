#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone

from typing import Dict, Any

from tonga.models.records.event import BaseEvent

__all__ = [
    'CoffeeOrdered'
]


class CoffeeOrdered(BaseEvent):
    uuid: str
    cup_type: str
    coffee_type: str
    coffee_for: str
    amount: float

    def __init__(self, uuid: str, cup_type: str, coffee_type: str, coffee_for: str, amount: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.cup_type = cup_type
        self.coffee_type = coffee_type
        self.coffee_for = coffee_for
        self.amount = amount

    def to_dict(self) -> Dict[str, Any]:
        r_dict = self.base_dict()
        r_dict['uuid'] = self.uuid
        r_dict['cup_type'] = self.cup_type
        r_dict['coffee_type'] = self.coffee_type
        r_dict['coffee_for'] = self.coffee_for
        r_dict['amount'] = self.amount
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
                   cup_type=dict_data['cup_type'],
                   coffee_type=dict_data['coffee_type'],
                   coffee_for=dict_data['coffee_for'],
                   amount=dict_data['amount'])

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeOrdered'
