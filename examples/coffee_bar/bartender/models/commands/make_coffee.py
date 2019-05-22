#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aioevent import BaseCommand
from migration.app import BaseApp

from typing import Dict, Any

__all__ = [
    'MakeCoffee'
]


class MakeCoffee(BaseCommand):
    uuid: str
    cup_type: str
    coffee_type: str

    def __init__(self, uuid: str, cup_type: str, coffee_type: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.cup_type = cup_type
        self.coffee_type = coffee_type

    async def execute(self, app: BaseApp, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> None:
        pass

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.command.MakeCoffee'
