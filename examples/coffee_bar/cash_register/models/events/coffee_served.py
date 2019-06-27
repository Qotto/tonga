#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from tonga.models.records.event import BaseEvent

from typing import Dict, Any

__all__ = [
    'CoffeeServed'
]


class CoffeeServed(BaseEvent):
    uuid: str
    served_to: str
    is_payed: bool
    amount: float

    def __init__(self, uuid: str, served_to: str, is_payed: bool, amount: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.served_to = served_to
        self.is_payed = is_payed
        self.amount = amount

    def to_dict(self) -> Dict[str, Any]:
        r_dict = self.base_dict()
        r_dict['uuid'] = self.uuid
        r_dict['served_to'] = self.served_to
        r_dict['is_payed'] = self.is_payed
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
                   served_to=dict_data['served_to'],
                   is_payed=dict_data['is_payed'],
                   amount=dict_data['amount'])

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeServed'
