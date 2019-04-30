#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent
from aioevent.model.exceptions import KafkaProducerError

from typing import Dict, Any

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

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)
        try:
            context = self.context
            context['coffee_time'] = self.coffee_time
            app.get('waiter_state').coffee_awaiting(self.uuid, context)
        except KafkaProducerError('Fail to send event', 500) as err:
            raise err

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.bartender.event.CoffeeFinished'
