#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from tonga.models.events.event import BaseEvent

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

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeServed'
