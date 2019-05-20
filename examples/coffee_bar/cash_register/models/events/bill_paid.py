#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent

from typing import Dict, Any


__all__ = [
    'BillPaid'
]


class BillPaid(BaseEvent):
    uuid: str
    coffee_uuid: str
    amount: float

    def __init__(self, uuid: str, coffee_uuid: str, amount: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.coffee_uuid = coffee_uuid
        self.amount = amount

    async def handle(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        print(self.__dict__)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.cashregister.event.BillPaid'
