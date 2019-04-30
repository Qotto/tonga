#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka.producer.message_accumulator import BatchBuilder

from typing import List

from aioevent.model import BaseModel

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

    async def send_and_await(self, event: BaseModel, topic: str) -> None:
        raise NotImplementedError

    def create_batch(self) -> BatchBuilder:
        raise NotImplementedError

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        raise NotImplementedError

    async def partitions_by_topic(self, topic: str) -> List[str]:
        raise NotImplementedError

    async def get_transactional_producer(self, transactional_uuid: str):
        raise NotImplementedError
