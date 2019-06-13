#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

# Import BaseEvent
from tonga.models.records.event import BaseEvent

from typing import Dict, Any

__all__ = [
    'CoffeeFinished'
]


class CoffeeFinished(BaseEvent):
    uuid: str
    coffee_for: str
    coffee_time: float

    def __init__(self, uuid: str, coffee_for: str, coffee_time: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.coffee_for = coffee_for
        self.coffee_time = coffee_time

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.bartender.event.CoffeeFinished'
