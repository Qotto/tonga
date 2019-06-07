#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


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
    pass


class AioKafkaProducerBadParams(ValueError):
    """AioKafkaProducerBadParams

    This error was raised when producer was call with bad params
    """
    pass


class KafkaProducerError(RuntimeError):
    """KafkaProducerError

    This error was raised when some generic error was raised form Aiokafka
    """
    pass


class KafkaProducerNotStartedError(RuntimeError):
    """KafkaProducerNotStartedError

    This error was raised when producer was not started
    """
    pass


class KafkaProducerAlreadyStartedError(RuntimeError):
    """KafkaProducerAlreadyStartedError

    This error was raised when producer was already started
    """
    pass


class KafkaProducerTimeoutError(TimeoutError):
    """KafkaProducerTimeoutError

    This error was raised when producer timeout on broker
    """
    pass


class KeyErrorSendEvent(KeyError):
    """KeyErrorSendEvent

    This error was raised when KeyError was raised
    """
    pass


class ValueErrorSendEvent(ValueError):
    """ValueErrorSendEvent

    This error was raised when ValueError was raised
    """
    pass


class TypeErrorSendEvent(TypeError):
    """TypeErrorSendEvent

    This error was raised when TypeError was raised
    """
    pass


class FailToSendEvent(Exception):
    """FailToSendEvent

    This error was raised when producer fail to send event
    """
    pass


class FailToSendBatch(Exception):
    """FailToSendBatch

    This error was raised when producer fail to send batch
    """
    pass


class UnknownEventBase(TypeError):
    """UnknownEventBase

    This error was raised when producer receive an unknown BaseEvent
    """
    pass
