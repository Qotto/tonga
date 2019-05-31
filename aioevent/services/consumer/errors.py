#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

__all__ = [
    'ConsumerConnectionError',
    'AioKafkaConsumerBadParams',
    'KafkaConsumerError',
    'KafkaConsumerNotStartedError',
    'KafkaConsumerAlreadyStartedError',
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


class ConsumerConnectionError(ConnectionError):
    pass


class AioKafkaConsumerBadParams(ValueError):
    pass


class KafkaConsumerError(RuntimeError):
    pass


class KafkaConsumerNotStartedError(RuntimeError):
    pass


class KafkaConsumerAlreadyStartedError(RuntimeError):
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


class UnknownHandler(TypeError):
    pass


class UnknownHandlerReturn(TypeError):
    pass


class UnknownStoreRecordHandler(TypeError):
    pass


class HandlerException(Exception):
    pass
