#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contains all producer errors
"""

__all__ = [
    'ProducerConnectionError',
    'AioKafkaProducerBadParams',
    'KafkaProducerError',
    'KafkaProducerNotStartedError',
    'KafkaProducerAlreadyStartedError',
    'KafkaProducerTimeoutError',
    'KeyErrorSendEvent',
    'ValueErrorSendEvent',
    'TypeErrorSendEvent',
    'FailToSendEvent',
    'UnknownEventBase',
    'FailToSendBatch',
]


class ProducerConnectionError(ConnectionError):
    """ProducerConnectionError

    This error was raised when producer can't connect to broker
    """


class AioKafkaProducerBadParams(ValueError):
    """AioKafkaProducerBadParams

    This error was raised when producer was call with bad params
    """


class KafkaProducerError(RuntimeError):
    """KafkaProducerError

    This error was raised when some generic error was raised form Aiokafka
    """


class KafkaProducerNotStartedError(RuntimeError):
    """KafkaProducerNotStartedError

    This error was raised when producer was not started
    """


class KafkaProducerAlreadyStartedError(RuntimeError):
    """KafkaProducerAlreadyStartedError

    This error was raised when producer was already started
    """


class KafkaProducerTimeoutError(TimeoutError):
    """KafkaProducerTimeoutError

    This error was raised when producer timeout on broker
    """


class KeyErrorSendEvent(KeyError):
    """KeyErrorSendEvent

    This error was raised when KeyError was raised
    """


class ValueErrorSendEvent(ValueError):
    """ValueErrorSendEvent

    This error was raised when ValueError was raised
    """


class TypeErrorSendEvent(TypeError):
    """TypeErrorSendEvent

    This error was raised when TypeError was raised
    """


class FailToSendEvent(Exception):
    """FailToSendEvent

    This error was raised when producer fail to send event
    """


class FailToSendBatch(Exception):
    """FailToSendBatch

    This error was raised when producer fail to send batch
    """


class UnknownEventBase(TypeError):
    """UnknownEventBase

    This error was raised when producer receive an unknown BaseEvent
    """
