#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from aiokafka import TopicPartition

from typing import Optional

# Import BaseEvent
from tonga.models.events.event import BaseEvent
# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
# Import BaseProducer
from tonga.services.producer.base import BaseProducer

# Import MakeCoffee command
from examples.coffee_bar.bartender.models.commands.make_coffee import MakeCoffee


class BillCreatedHandler(BaseEventHandler):
    _transactional_producer: BaseProducer

    def __init__(self, producer: BaseProducer, **kwargs):
        super().__init__(**kwargs)
        self._transactional_producer = producer

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Optional[str]:
        if not self._transactional_producer.is_running():
            await self._transactional_producer.start_producer()

        async with self._transactional_producer.init_transaction():
            # Creates commit_offsets dict
            commit_offsets = {tp: offset + 1}

            # Creates & enrich context
            context = event.context
            context['start_time'] = datetime.now(timezone.utc).timestamp()
            context['bill_uuid'] = event.uuid
            context['amount'] = event.amount

            # Creates MakeCoffee command
            make_coffee = MakeCoffee(event.coffee_uuid, cup_type=event.context['cup_type'],
                                     coffee_type=event.context['coffee_type'], context=context,
                                     processing_guarantee='at_most_once')

            # Sends MakeCoffee command
            await self._transactional_producer.send_and_await(make_coffee, 'coffee-maker-commands')

            # End transaction
            await self._transactional_producer.end_transaction(commit_offsets, group_id)
        # TODO raise an exception
        return 'transaction'

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.cashregister.event.BillCreated'
