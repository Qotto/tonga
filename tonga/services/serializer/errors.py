#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

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
    pass


class AvroEncodeError(Exception):
    pass


class AvroDecodeError(Exception):
    pass


class NotMatchedName(NameError):
    pass


class MissingEventClass(NameError):
    pass


class MissingHandlerClass(NameError):
    pass

# ----------- End Avro Exceptions -----------

# ----------- Start KafkaKey Exceptions -----------


class KeySerializerDecodeError(ValueError):
    pass


class KeySerializerEncodeError(ValueError):
    pass

# ----------- End KafkaKey Exceptions -----------
