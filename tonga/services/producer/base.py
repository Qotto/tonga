#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.producer.producer import TransactionContext

from typing import Dict

from tonga.models import BaseModel

__all__ = [
    'BaseProducer',
]


class BaseProducer:
    def __init__(self) -> None:
        pass

    async def start_producer(self) -> None:
        raise NotImplementedError

    async def stop_producer(self) -> None:
        raise NotImplementedError

    def is_running(self) -> bool:
        raise NotImplementedError

    async def send_and_await(self, event: BaseModel, topic: str) -> None:
        raise NotImplementedError

    def create_batch(self) -> BatchBuilder:
        raise NotImplementedError

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        raise NotImplementedError

    def init_transaction(self) -> TransactionContext:
        raise NotImplementedError

    async def end_transaction(self, committed_offsets: Dict[TopicPartition, int], group_id: str) -> None:
        raise NotImplementedError

