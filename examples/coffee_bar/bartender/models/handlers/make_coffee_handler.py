#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

# Import BaseCommandHandler
from aioevent.models.handlers.command.command_handler import BaseCommandHandler
# Import BaseCommand
from aioevent.models.events.command.command import BaseCommand

from typing import Union


class MakeCoffeeHandler(BaseCommandHandler):
    def __int__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    async def execute(self, event: BaseCommand, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        pass

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.coffeemaker.command.MakeCoffee'
