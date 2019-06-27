#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from tonga.models.records.event import BaseEvent

from typing import Dict, Any


__all__ = [
    'BillCreated'
]


class BillCreated(BaseEvent):
    uuid: str
    coffee_uuid: str
    amount: float

    def __init__(self, uuid: str, coffee_uuid: str, amount: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.coffee_uuid = coffee_uuid
        self.amount = amount

    def to_dict(self) -> Dict[str, Any]:
        r_dict = self.base_dict()
        r_dict['uuid'] = self.uuid
        r_dict['coffee_uuid'] = self.coffee_uuid
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
                   coffee_uuid=dict_data['coffee_uuid'],
                   amount=dict_data['amount'])

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.cashregister.event.BillCreated'
