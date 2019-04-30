#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent
from aioevent.model.exceptions import KafkaProducerError

from typing import Dict, Any

from ...models.coffee import Coffee

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

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)
        new_order = Coffee(uuid=self.uuid, cup_type=self.cup_type,
                           coffee_for=self.coffee_for, coffee_type=self.coffee_type, amount=self.amount)
        app.get('waiter_state').add_ordered_coffee(new_order)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeOrdered'
