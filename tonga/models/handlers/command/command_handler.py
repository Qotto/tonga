#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from tonga.models.handlers.base import BaseHandler
from tonga.models.events.command.command import BaseCommand

__all__ = [
    'BaseCommandHandler'
]


class BaseCommandHandler(BaseHandler):
    @classmethod
    def handler_name(cls) -> str:
        raise NotImplementedError

    async def execute(self, event: BaseCommand, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
