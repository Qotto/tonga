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
    pass


class AioKafkaProducerBadParams(ValueError):
    pass


class KafkaProducerError(RuntimeError):
    pass


class KafkaProducerNotStartedError(RuntimeError):
    pass


class KafkaProducerAlreadyStartedError(RuntimeError):
    pass


class KafkaProducerTimeoutError(TimeoutError):
    pass


class KeyErrorSendEvent(KeyError):
    pass


class ValueErrorSendEvent(ValueError):
    pass


class TypeErrorSendEvent(TypeError):
    pass


class FailToSendEvent(Exception):
    pass


class FailToSendBatch(Exception):
    pass


class UnknownEventBase(TypeError):
    pass
