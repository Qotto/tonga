#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Type

# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
# Import StoreBuilderBase
from tonga.stores.manager.kafka_store_manager import KafkaStoreManager
# Import BaseProducer
from tonga.services.producer.base import BaseProducer

# Import Coffee Model
from examples.coffee_bar.cash_register.models.events.coffee_served import CoffeeServed
from examples.coffee_bar.cash_register.models.events.bill_paid import BillPaid
from examples.coffee_bar.cash_register.models.bill import Bill


class CoffeeServedHandler(BaseEventHandler):
    _store_builder: KafkaStoreManager
    _transactional_producer: BaseProducer

    def __init__(self, store_builder: KafkaStoreManager, transactional_producer: BaseProducer):
        self._store_builder = store_builder
        self._transactional_producer = transactional_producer

    async def handle(self, event: Type[CoffeeServed]) -> None:
        # Gets bill in local store
        bill = Bill.__from_bytes_dict__(await self._store_builder.get_from_local_store(event.context['bill_uuid']))

        # Updates bill
        bill.set_is_paid(True)
        bill.set_context(event.context)

        # Sets updated bill on cash register local store
        await self._store_builder.set_from_local_store(bill.uuid, bill.__to_bytes_dict__())

        # Creates BillPaid event
        bill_paid = BillPaid(uuid=bill.uuid, coffee_uuid=bill.coffee_uuid, amount=bill.amount, context=bill.context)

        # Sends BillCreated event
        await self._transactional_producer.send_and_wait(bill_paid, 'cash-register-events')

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeServed'
