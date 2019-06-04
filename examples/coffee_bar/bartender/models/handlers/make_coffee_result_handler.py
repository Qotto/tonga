#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from aiokafka import TopicPartition

# Import BaseCommandHandler
from tonga.models.handlers.result.result_handler import BaseResultHandler
# Import BaseCommand
from tonga.models.events.result.result import BaseResult
# Import BaseProducer
from tonga.services.producer.base import BaseProducer

from typing import Union

# Import CoffeeFinished event
from examples.coffee_bar.bartender.models.events.coffee_finished import CoffeeFinished


class MakeCoffeeResultHandler(BaseResultHandler):
    _transactional_producer: BaseProducer

    def __init__(self, producer: BaseProducer, **kwargs) -> None:
        super().__init__(**kwargs)
        self._transactional_producer = producer

    async def on_result(self, result: BaseResult, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        if not self._transactional_producer.is_running():
            await self._transactional_producer.start_producer()

        # Checks error
        if result.error is not None:
            # Do stuff if an error as been reached
            return 'error'

        async with self._transactional_producer.init_transaction():
            # Creates commit_offsets dict
            commit_offsets = {tp: offset + 1}

            # Creates & enrich context
            context = result.context
            context['coffee_finished'] = datetime.now(timezone.utc).timestamp()
            delta_coffee_time = (datetime.fromtimestamp(context['start_time'], timezone.utc) -
                                 datetime.fromtimestamp(context['coffee_finished'], timezone.utc)). \
                total_seconds()

            # Creates CoffeeFinished event
            coffee_finished = CoffeeFinished(result.uuid, coffee_for=result.context['coffee_for'],
                                             coffee_time=delta_coffee_time, context=context)

            # Sends CoffeeFinished event
            await self._transactional_producer.send_and_await(coffee_finished, 'bartender-events')

            # End transaction
            await self._transactional_producer.end_transaction(commit_offsets, group_id)
        # TODO raise an exception
        return 'transaction'

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.coffeemaker.result.MakeCoffeeResult'
