#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Optional

# Import BaseEvent
from tonga.models.records.event import BaseEvent
# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler


class CoffeeFinishedHandler(BaseEventHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Optional[str]:
        pass

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.bartender.event.CoffeeFinished'
