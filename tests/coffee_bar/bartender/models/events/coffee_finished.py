#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent
from aioevent.app.base import BaseApp

from typing import Dict, Any

__all__ = [
    'CoffeeFinished'
]


class CoffeeFinished(BaseEvent):
    uuid: str
    cup_type: str
    coffee_type: str
    coffee_for: str
    coffee_time: float

    def __init__(self, uuid: str, cup_type: str, coffee_type: str, coffee_for: str, coffee_time: float,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.cup_type = cup_type
        self.coffee_type = coffee_type
        self.coffee_for = coffee_for
        self.coffee_time = coffee_time

    async def handle(self, app: BaseApp, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.bartender.event.CoffeeFinished'
