#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent
from aioevent.model.exceptions import KafkaProducerError, KafkaConsumerError

from typing import Dict, Any

from .bill_created import BillCreated
from ..bill import Bill

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
        print(f'Create Event {uuid}')
        self.uuid = uuid
        self.cup_type = cup_type
        self.coffee_type = coffee_type
        self.coffee_for = coffee_for
        self.amount = amount

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        try:
            context = self.context
            context['coffee_type'] = self.coffee_type
            context['cup_type'] = self.cup_type
            context['coffee_for'] = self.coffee_for

            bill = Bill(self.uuid, self.amount)
            bill.set_context(context)

            app.get('cash_register_repository').add_bill(bill)

            event = BillCreated(bill.uuid, bill.coffee_uuid, bill.amount, context=bill.context)

            await app.producers['cash_register_producer'].send_and_await(event, 'cash-register-events')
        except KafkaProducerError:
            raise KafkaConsumerError(f'Fail to send event BillCreated', 500)
        except Exception as e:
            raise e

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        print('From_data')
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeOrdered'
