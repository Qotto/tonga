#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any

from aioevent.models.events.event import BaseEvent

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

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.bartender.event.CoffeeFinished'
