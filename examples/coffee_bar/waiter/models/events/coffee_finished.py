#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent
from aioevent.model.exceptions import KafkaConsumerError

from typing import Dict, Any

from examples.coffee_bar.waiter.repository.base import NotFound

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

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        try:
            coffee = app.get('waiter_local_repository').get_coffee_by_uuid(self.uuid)
            coffee.set_context(self.context)
            coffee.set_state('awaiting')
            app.get('waiter_local_repository').upd_coffee(coffee)
        except NotFound:
            raise KafkaConsumerError(f'Fail to find coffee uuid : {self.uuid}', 404)

    async def state_builder(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition,
                            offset: int) -> None:
        try:
            coffee = app.get('waiter_global_repository').get_coffee_by_uuid(self.uuid)
            coffee.set_context(self.context)
            coffee.set_state('awaiting')
            app.get('waiter_global_repository').upd_coffee(coffee)
        except NotFound:
            raise KafkaConsumerError(f'Fail to find coffee uuid : {self.uuid}', 404)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.bartender.event.CoffeeFinished'
