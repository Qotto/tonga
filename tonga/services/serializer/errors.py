#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contains all Serializer errors
"""

__all__ = [
    'AvroAlreadyRegister',
    'AvroEncodeError',
    'AvroDecodeError',
    'NotMatchedName',
    'MissingEventClass',
    'MissingHandlerClass',
    'KeySerializerDecodeError',
    'KeySerializerEncodeError'
]

# ----------- Start Avro Exceptions -----------


class AvroAlreadyRegister(Exception):
    """AvroAlreadyRegister

    This error was raised when AvroSerializer as already register the Avro schema
    """


class AvroEncodeError(Exception):
    """AvroEncodeError

    This error was raised when AvroSerializer try to encode an BaseModel class and fail
    """


class AvroDecodeError(Exception):
    """AvroDecodeError

    This error was raised when AvroSerializer try to decode an BaseModel class and fail
    """


class NotMatchedName(NameError):
    """NotMatchedName

    This error was raised when AvroSerializer can't find same name in registered schema
    """


class MissingEventClass(NameError):
    """MissingEventClass

    This error was raised when AvroSerializer can't find BaseModel in own registered BaseModel list
    """


class MissingHandlerClass(NameError):
    """MissingHandlerClass

    This error was raised when AvroSerializer can't find BaseHandlerModel in own registered BaseHandlerModel list
    """

# ----------- End Avro Exceptions -----------

# ----------- Start KafkaKey Exceptions -----------


class KeySerializerDecodeError(ValueError):
    """KeySerializerDecodeError

    This error was raised when KafkaKeySerializer can't decode key
    """


class KeySerializerEncodeError(ValueError):
    """KeySerializerEncodeError

    This error was raised when KafkaKeySerializer can't encode key
    """

# ----------- End KafkaKey Exceptions -----------
