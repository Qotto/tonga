#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from tonga.models.records.event.event import BaseEvent

from typing import Dict, Any

__all__ = [
    'CoffeeStarted'
]


class CoffeeStarted(BaseEvent):
    uuid: str

    def __init__(self, uuid: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.coffeemaker.event.CoffeeStarted'
