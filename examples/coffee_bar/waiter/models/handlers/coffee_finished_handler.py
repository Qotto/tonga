#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import asyncio
from aiokafka import TopicPartition

from typing import Optional

# Import BaseEvent
from aioevent.models.events.event import BaseEvent
# Import BaseEventHandler
from aioevent.models.handler.event.event_handler import BaseEventHandler
# Import StoreBuilderBase
from aioevent.stores.store_builder.base import BaseStoreBuilder
# Import store exceptions
from aioevent.models.exceptions.exceptions import StoreKeyNotFound

# Import Coffee Model
from examples.coffee_bar.waiter.models.coffee import Coffee


class CoffeeFinishedHandler(BaseEventHandler):
    _store_builder: BaseStoreBuilder

    def __init__(self, store_builder: BaseStoreBuilder, **kwargs):
        super().__init__(**kwargs)
        self._store_builder = store_builder

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Optional[str]:
        coffee = None
        for i in range(0, 10):
            try:
                coffee = Coffee.__from_dict_bytes__(await self._store_builder.get_from_local_store(event.uuid))
                break
            except StoreKeyNotFound:
                await asyncio.sleep(1)
        if coffee is not None:
            coffee.set_state('awaiting')
            coffee.set_context(event.context)
            await self._store_builder.set_from_local_store(event.uuid, coffee.__to_bytes_dict__())
            return None
         # TODO raise an exception

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.bartender.event.CoffeeFinished'
