#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from kafka.errors import KafkaConfigurationError

__all__ = [
    'BadArgumentKafkaClient',
    'CurrentInstanceOutOfRange',
    'KafkaAdminConfigurationError',
    'KafkaClientConnectionErrors'
]


class BadArgumentKafkaClient(ValueError):
    """BadArgumentKafkaClient

    This error was raised when bad argument was send to KafkaClient
    """


class CurrentInstanceOutOfRange(IndexError):
    """CurrentInstanceOutOfRange

    This error was raised when current instance is higher than number of replica
    """


class KafkaAdminConfigurationError(KafkaConfigurationError):
    """KafkaAdminConfigurationError

    This error was raised when KafkaConfigurationError was raised (bad params)
    """


class KafkaClientConnectionErrors(ConnectionError):
    """CurrentInstanceOutOfRange

    This error was raised when current instance is higher than number of replica
    """
