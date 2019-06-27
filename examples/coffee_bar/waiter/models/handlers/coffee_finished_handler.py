#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

# Import BaseEventHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
# Import StoreBuilderBase
from tonga.stores.manager.kafka_store_manager import KafkaStoreManager

# Import TransactionManager
from examples.coffee_bar.waiter import transactional_manager
# Import CoffeeFinished event model
from examples.coffee_bar.waiter.models.events.coffee_finished import CoffeeFinished
# Import Coffee Model
from examples.coffee_bar.waiter.models.coffee import Coffee


class CoffeeFinishedHandler(BaseEventHandler):
    _store_builder: KafkaStoreManager

    def __init__(self, store_builder: KafkaStoreManager):
        self._store_builder = store_builder

    @transactional_manager
    async def handle(self, event: CoffeeFinished) -> None:
        coffee = Coffee.__from_dict_bytes__(await self._store_builder.get_from_local_store(event.uuid))

        # Updates coffee
        coffee.set_state('awaiting')
        coffee.set_context(event.context)

        # Sets coffee in local store
        await self._store_builder.set_from_local_store(event.uuid, coffee.__to_bytes_dict__())

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.bartender.event.CoffeeFinished'
