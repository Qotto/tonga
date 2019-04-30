#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent

from typing import Dict, Any

__all__ = [
    'CoffeeStarted'
]


class CoffeeStarted(BaseEvent):
    uuid: str
    cup_type: str
    coffee_type: str
    coffee_for: str

    def __init__(self, uuid: str, coffee_for: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.coffee_for = coffee_for

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.events.CoffeeStarted'
