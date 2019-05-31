#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any

from aioevent.models.events.event import BaseEvent

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

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeOrdered'
