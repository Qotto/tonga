#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Optional

# Import BaseEvent
from aioevent.models.events.event import BaseEvent
# Import BaseEventHandler
from aioevent.models.handler.event.event_handler import BaseEventHandler
# Import StoreBuilderBase
from aioevent.stores.store_builder.base import BaseStoreBuilder

# Import Coffee Model
from examples.coffee_bar.cash_register.models.events.bill_paid import BillPaid
from examples.coffee_bar.cash_register.models.bill import Bill


class CoffeeServedHandler(BaseEventHandler):
    _store_builder: BaseStoreBuilder

    def __init__(self, store_builder: BaseStoreBuilder, **kwargs):
        super().__init__(**kwargs)
        self._store_builder = store_builder

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Optional[str]:
        async with self._store_builder.init_transaction():
            # Creates commit_offsets dict
            commit_offsets = {tp: offset + 1}

            # Gets bill in local store
            bill = Bill.__from_bytes_dict__(await self._store_builder.get_from_local_store(event.context['bill_uuid']))

            # Updates bill
            bill.set_is_paid(True)
            bill.set_context(event.context)

            # Sets updated bill on cash register local store
            await self._store_builder.set_from_local_store(bill.uuid, bill.__to_bytes_dict__())

            # Creates BillPaid event
            bill_paid = BillPaid(uuid=bill.uuid, coffee_uuid=bill.coffee_uuid, amount=bill.amount,
                                 context=bill.context)

            # Sends BillCreated event
            await self._store_builder.get_producer().send_and_await(bill_paid, 'cash-register-events')

            # End transaction
            await self._store_builder.end_transaction(commit_offsets, group_id)
        # TODO raise an exception
        return 'transaction'

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.waiter.event.CoffeeServed'
