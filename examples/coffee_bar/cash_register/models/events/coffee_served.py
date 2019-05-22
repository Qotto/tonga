#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent
from aioevent.models.exceptions import KafkaProducerError, KafkaConsumerError

from typing import Dict, Any

from .bill_paid import BillPaid
from ...repository.base import NotFound

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

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        try:
            bill = app.get('cash_register_repository').get_bill_by_uuid(self.context['bill_uuid'])
            bill.set_is_paid(True)
            bill.set_context(self.context)
            event = BillPaid(bill.uuid, bill.coffee_uuid, bill.amount, context=bill.context)
            await app.producers['cash_register_producer'].send_and_await(event, 'cash-register-events')
            app.get('cash_register_repository').upd_bill(bill)
        except KafkaProducerError as err:
            raise KafkaConsumerError(f'Fail to send BillPaid', 500)
        except NotFound:
            raise KafkaProducerError(f'Fail to find bill uuid : {self.context["bill_uuid"]}', 404)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeServed'
