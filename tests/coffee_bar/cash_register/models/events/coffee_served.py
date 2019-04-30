#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent
from aioevent.model.exceptions import KafkaProducerError

from typing import Dict, Any

from .bill_paid import BillPaid

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

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)
        try:
            event = BillPaid(self.context['bill_uuid'], self.uuid, self.amount, context=self.context)
            await app.producers['cash_register_producer'].send_and_await(event, 'cash-register-events')
        except KafkaProducerError('Fail to send event', 500) as err:
            raise err

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeServed'
