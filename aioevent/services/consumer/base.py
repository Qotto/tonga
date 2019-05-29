#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from typing import List, Dict


__all__ = [
    'BaseConsumer',
    'ConsumerConnectionError',
    'AioKafkaConsumerBadParams',
    'KafkaConsumerError',
    'BadSerializer',
    'ConsumerKafkaTimeoutError',
    'IllegalOperation',
    'TopicPartitionError',
    'NoPartitionAssigned',
    'OffsetError',
    'UnknownHandler',
    'UnknownStoreRecordHandler',
    'UnknownHandlerReturn',
    'HandlerException',
]


class BaseConsumer:
    def __init__(self) -> None:
        pass

    async def start_consumer(self) -> None:
        raise NotImplementedError

    async def stop_consumer(self) -> None:
        raise NotImplementedError

    def is_running(self) -> bool:
        raise NotImplementedError

    async def listen_event(self, mod: str = 'latest') -> None:
        raise NotImplementedError

    async def listen_store_records(self, rebuild: bool = False) -> None:
        raise NotImplementedError

    async def get_many(self, partitions: List[int] = None, max_records: int = None):
        raise NotImplementedError

    async def get_one(self, partitions: List[int] = None):
        raise NotImplementedError


class ConsumerConnectionError(ConnectionError):
    pass


class AioKafkaConsumerBadParams(ValueError):
    pass


class KafkaConsumerError(RuntimeError):
    pass


class ConsumerKafkaTimeoutError(TimeoutError):
    pass


class IllegalOperation(TimeoutError):
    pass


class TopicPartitionError(TypeError):
    pass


class OffsetError(TypeError):
    pass


class NoPartitionAssigned(TypeError):
    pass


class BadSerializer(TypeError):
    pass


class UnknownHandler(TypeError):
    pass


class UnknownHandlerReturn(TypeError):
    pass


class UnknownStoreRecordHandler(TypeError):
    pass


class HandlerException(Exception):
    pass
