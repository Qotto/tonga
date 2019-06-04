#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from tonga.services.consumer.base import BaseConsumer
from tonga.services.producer.base import BaseProducer
from tonga.stores.local.base import BaseLocalStore
from tonga.stores.globall.base import BaseGlobalStore

__all__ = [
    'BaseStoreBuilder'
]


class BaseStoreBuilder:
    async def initialize_store_builder(self) -> None:
        raise NotImplementedError

    # Get stores
    def get_local_store(self) -> BaseLocalStore:
        raise NotImplementedError

    def get_global_store(self) -> BaseGlobalStore:
        raise NotImplementedError

    # Get info
    def get_current_instance(self) -> int:
        raise NotImplementedError

    def is_event_sourcing(self) -> bool:
        raise NotImplementedError

    # Get producer & consumer
    def get_producer(self) -> BaseProducer:
        raise NotImplementedError

    def get_consumer(self) -> BaseConsumer:
        raise NotImplementedError

    # Functions for local store management
    async def set_from_local_store(self, key: str, value: bytes) -> None:
        raise NotImplementedError

    async def get_from_local_store(self, key: str) -> bytes:
        raise NotImplementedError

    async def delete_from_local_store(self, key: str) -> None:
        raise NotImplementedError

    async def update_metadata_from_local_store(self, tp: TopicPartition, offset: int) -> None:
        raise NotImplementedError

    async def set_from_local_store_rebuild(self, key: str, value: bytes) -> None:
        raise NotImplementedError

    async def delete_from_local_store_rebuild(self, key: str) -> None:
        raise NotImplementedError

    # Function for global store management
    async def get_from_global_store(self, key: str) -> bytes:
        raise NotImplementedError

    async def set_from_global_store(self, key: str, value: bytes) -> None:
        raise NotImplementedError

    async def delete_from_global_store(self, key: str) -> None:
        raise NotImplementedError

    async def update_metadata_from_global_store(self, tp: TopicPartition, offset: int) -> None:
        raise NotImplementedError
