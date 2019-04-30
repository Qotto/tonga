#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from aiokafka import TopicPartition
from aioevent import BaseEvent
from aioevent import AioEvent
from aioevent.model.exceptions import KafkaProducerError

from typing import Dict, Any

from ..commands import MakeCoffee

__all__ = [
    'BillCreated'
]


class BillCreated(BaseEvent):
    uuid: str
    coffee_uuid: str
    amount: bytes

    def __init__(self, uuid: str, coffee_uuid: str, amount: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.coffee_uuid = coffee_uuid
        self.amount = amount

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)
        try:
            context = self.context
            context['start_time'] = datetime.now(timezone.utc).timestamp()
            context['bill_uuid'] = self.uuid
            context['amount'] = self.amount

            event = MakeCoffee(self.coffee_uuid, cup_type=self.context['cup_type'],
                               coffee_type=self.context['coffee_type'], context=context,
                               processing_guarantee='at_most_once')
            print(event.__dict__)
            print('before')
            await app.producers['bartender_producer'].send_and_await(event, 'coffee-maker-commands')
        except KafkaProducerError as er:
            print('er = ', er)
        except Exception as e:
            print('e', e)
            raise e

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.cashregister.event.BillCreated'
