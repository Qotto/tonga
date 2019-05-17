#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent

from typing import Dict, Any

from tests.coffee_bar.waiter.models.coffee import Coffee

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

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        coffee = Coffee(self.coffee_type, self.cup_type, self.coffee_for, self.amount, uuid=self.uuid)
        app.get('waiter_local_repository').add_coffee(coffee)

    async def state_builder(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition,
                            offset: int) -> None:
        coffee = Coffee(self.coffee_type, self.cup_type, self.coffee_for, self.amount, uuid=self.uuid)
        app.get('waiter_global_repository').add_coffee(coffee)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeOrdered'
