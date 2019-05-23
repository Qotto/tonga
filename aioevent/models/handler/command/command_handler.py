#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from aioevent.models.handler.base import BaseHandler
from aioevent.models.events.base import BaseModel

__all__ = [
    'BaseCommandHandler'
]


class BaseCommandHandler(BaseHandler):
    @classmethod
    def handler_name(cls) -> str:
        raise NotImplementedError

    async def execute(self, event: BaseModel, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError