#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Optional

# Import BaseEvent
from tonga.models.events.event import BaseEvent
# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
# Import StoreBuilderBase
from tonga.stores.store_builder.base import BaseStoreBuilder
# Import BaseProducer
from tonga.services.producer.base import BaseProducer

# Import Coffee Model
from examples.coffee_bar.cash_register.models.events.bill_created import BillCreated
from examples.coffee_bar.cash_register.models.bill import Bill


class CoffeeOrderedHandler(BaseEventHandler):
    _store_builder: BaseStoreBuilder
    _transactional_producer: BaseProducer

    def __init__(self, store_builder: BaseStoreBuilder, transactional_producer: BaseProducer, **kwargs):
        super().__init__(**kwargs)
        self._store_builder = store_builder
        self._transactional_producer = transactional_producer

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Optional[str]:
        if not self._transactional_producer.is_running():
            await self._transactional_producer.start_producer()

        async with self._transactional_producer.init_transaction():
            # Creates commit_offsets dict
            commit_offsets = {tp: offset + 1}
            # Instances bill class
            bill = Bill(coffee_uuid=event.uuid, amount=event.amount)

            # Sets on cash register local store
            await self._store_builder.set_from_local_store(bill.uuid, bill.__to_bytes_dict__())

            # Creates BillCreated event
            bill_created = BillCreated(uuid=bill.uuid, coffee_uuid=bill.coffee_uuid, amount=bill.amount,
                                       context=bill.context)

            # Sends BillCreated event
            await self._transactional_producer.send_and_await(bill_created, 'cash-register-events')

            # End transaction
            await self._transactional_producer.end_transaction(commit_offsets, group_id)
        # TODO raise an exception
        return 'transaction'

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeOrdered'
