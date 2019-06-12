#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain all consumer errors
"""

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
    """ConsumerConnectionError

    This error was raised when consumer can't connect to Kafka broker
    """


class AioKafkaConsumerBadParams(ValueError):
    """AioKafkaConsumerBadParams

    This error was raised when consumer was call with bad params
    """


class KafkaConsumerError(RuntimeError):
    """KafkaConsumerError

    This error was raised when an generic error from aiokafka was raised
    """


class KafkaConsumerNotStartedError(RuntimeError):
    """KafkaConsumerNotStartedError

    This error was raised when consumer was not started
    """


class KafkaConsumerAlreadyStartedError(RuntimeError):
    """KafkaConsumerAlreadyStartedError

    This error was raised when consumer was already started
    """


class ConsumerKafkaTimeoutError(TimeoutError):
    """ConsumerKafkaTimeoutError

    This error was raised when Tonga timeout on Kafka broker
    """


class IllegalOperation(TimeoutError):
    """IllegalOperation

    This error was raised when topics / partition doesn't exist
    """


class TopicPartitionError(TypeError):
    """TopicPartitionError

    This error was raised topics exist but not desired partition
    """


class OffsetError(TypeError):
    """OffsetError

    This error was raised when offset was out of range
    """


class NoPartitionAssigned(TypeError):
    """NoPartitionAssigned

    This error was raised when no partition was assigned to consumer
    """


class UnknownHandler(TypeError):
    """UnknownHandler

    This error was raised when consumer as an event but not handler was found
    """


class UnknownHandlerReturn(TypeError):
    """UnknownHandlerReturn

    This error was raised when handler return an unknown value
    """


class UnknownStoreRecordHandler(TypeError):
    """UnknownStoreRecordType

    This error was raised when store record handler was unknown
    """


class HandlerException(Exception):
    """HandlerException

    This error was raised when in an handler, consumer doesn't commit this message and retries with same handler,
    if 5 errors as been raised consumer stop listen event.
    """
