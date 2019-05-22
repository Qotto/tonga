#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from aioevent.models.events.event.event import BaseEvent
from aioevent.models.handler.event.event_handler import BaseEventHandler

from typing import Union

__all__ = [
    'TestEventHandler'
]


class TestEventHandler(BaseEventHandler):
    def __init__(self) -> None:
        pass

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.event.test'

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
