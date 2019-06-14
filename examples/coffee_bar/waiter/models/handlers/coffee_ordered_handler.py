#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from logging import getLogger
from aiokafka import TopicPartition

from typing import Optional

# Import BaseEvent
from tonga.models.records.event import BaseEvent
# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler

__all__ = [
    'CoffeeOrderedHandler'
]


class CoffeeOrderedHandler(BaseEventHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Optional[str]:
        logger = getLogger('tonga')
        logger.debug('Handler was called')
        return None

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeOrdered'
