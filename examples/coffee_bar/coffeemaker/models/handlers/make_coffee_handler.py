#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from aiokafka import TopicPartition

# Import BaseCommandHandler
from aioevent.models.handlers.command.command_handler import BaseCommandHandler
# Import BaseCommand
from aioevent.models.events.command.command import BaseCommand
# Import BaseProducer
from aioevent.services.producer.base import BaseProducer

from typing import Union

# Import MakeCoffeeResult / CoffeeStarted event
from examples.coffee_bar.coffeemaker.models.results.make_coffee_result import MakeCoffeeResult
from examples.coffee_bar.coffeemaker.models.events.coffee_started import CoffeeStarted


class MakeCoffeeHandler(BaseCommandHandler):
    _transactional_producer: BaseProducer

    def __init__(self, transactional_producer: BaseProducer, **kwargs) -> None:
        super().__init__(**kwargs)
        self._transactional_producer = transactional_producer

    async def execute(self, command: BaseCommand, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        if not self._transactional_producer.is_running():
            await self._transactional_producer.start_producer()

        async with self._transactional_producer.init_transaction():
            # Creates commit_offsets dict
            commit_offsets = {tp: offset + 1}

            # Creates CoffeeStarted event and MakeCoffeeResult result
            coffee_started = CoffeeStarted(command.uuid, context=command.context)
            make_coffee_result = MakeCoffeeResult(command.uuid, context=command.context)

            # Sends CoffeeFinished event
            await self._transactional_producer.send_and_await(coffee_started, 'coffee-maker-events')
            await self._transactional_producer.send_and_await(make_coffee_result, 'coffee-maker-results')

            # End transaction
            await self._transactional_producer.end_transaction(commit_offsets, group_id)
        # TODO raise an exception
        return 'transaction'

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.coffeemaker.command.MakeCoffee'
