#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
# Import StoreBuilderBase
from tonga.stores.manager.kafka_store_manager import KafkaStoreManager
# Import BaseProducer
from tonga.services.producer.base import BaseProducer
from tonga.services.coordinator.partitioner.key_partitioner import KeyPartitioner

# Import Coffee Model
from examples.coffee_bar.cash_register import transactional_manager
from examples.coffee_bar.cash_register.models.events.coffee_ordered import CoffeeOrdered
from examples.coffee_bar.cash_register.models.events.bill_created import BillCreated
from examples.coffee_bar.cash_register.models.bill import Bill


class CoffeeOrderedHandler(BaseEventHandler):
    _store_builder: KafkaStoreManager
    _transactional_producer: BaseProducer

    def __init__(self, store_builder: KafkaStoreManager, transactional_producer: BaseProducer):
        self._store_builder = store_builder
        self._transactional_producer = transactional_producer

    @transactional_manager
    async def handle(self, event: CoffeeOrdered) -> None:

        key_partitioner = KeyPartitioner()
        while True:
            bill = Bill(coffee_uuid=event.uuid, amount=event.amount)
            if key_partitioner(bill.uuid, [0, 1], [0, 1]) == self._store_builder.get_current_instance():
                break

        # Sets on cash register local store
        await self._store_builder.set_from_local_store(bill.uuid, bill.__to_bytes_dict__())

        # Creates BillCreated event
        bill_created = BillCreated(partition_key=bill.uuid, uuid=bill.uuid, coffee_uuid=bill.coffee_uuid,
                                   amount=bill.amount, context=bill.context)

        # Sends BillCreated event
        await self._transactional_producer.send_and_wait(bill_created, 'cash-register-events')

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.waiter.event.CoffeeOrdered'
