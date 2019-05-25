#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from aioevent.models.handler.base import BaseHandler
from aioevent.models.events.result.result import BaseResult

__all__ = [
    'BaseResultHandler',
]


class BaseResultHandler(BaseHandler):
    @classmethod
    def handler_name(cls) -> str:
        raise NotImplementedError

    async def on_result(self, event: BaseResult, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
