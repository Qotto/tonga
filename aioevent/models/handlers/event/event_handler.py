#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from aioevent.models.handlers.base import BaseHandler
from aioevent.models.events.base import BaseModel

__all__ = [
    'BaseEventHandler'
]


class BaseEventHandler(BaseHandler):
    @classmethod
    def handler_name(cls) -> str:
        raise NotImplementedError

    async def handle(self, event: BaseModel, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
