#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseEvent, AioEvent

from typing import Dict, Any

__all__ = [
    'MakeCoffeeResult'
]


class MakeCoffeeResult(BaseEvent):
    uuid: str

    def __init__(self, uuid: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid

    async def on_result(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        pass

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.result.MakeCoffeeResult'
