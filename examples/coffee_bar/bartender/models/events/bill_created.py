#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

# Import BaseEvent
from tonga.models.events.event import BaseEvent

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

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.cashregister.event.BillCreated'
