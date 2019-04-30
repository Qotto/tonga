#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from .base import AioEventBaseException

__all__ = [
    'AvroEncodeError',
    'AvroDecodeError',
    'CommandEventMissingProcessGuarantee',
    'BadSerializer',
    'KafkaConsumerError',
    'KafkaProducerError',
]


class AvroEncodeError(AioEventBaseException):
    """
    Raised when avro raise AvroTypeException

    Args:
        msg (str): Human readable string describing the exception.
        code (int): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """

    def __init__(self, msg: str, code: int) -> None:
        self.msg = msg
        self.code = code


class AvroDecodeError(AioEventBaseException):
    """
    Raised when avro raise AvroTypeException

    Args:
        msg (str): Human readable string describing the exception.
        code (int): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """

    def __init__(self, msg: str, code: int) -> None:
        self.msg = msg
        self.code = code


class CommandEventMissingProcessGuarantee(AioEventBaseException):
    """
    Raised when avro raise AvroTypeException

    Args:
        msg (str): Human readable string describing the exception.
        code (int): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """

    def __init__(self, msg: str, code: int) -> None:
        self.msg = msg
        self.code = code


class BadSerializer(AioEventBaseException):
    """
    Raised when avro raise AvroTypeException

    Args:
        msg (str): Human readable string describing the exception.
        code (int): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """

    def __init__(self, msg: str, code: int) -> None:
        self.msg = msg
        self.code = code


class KafkaProducerError(AioEventBaseException):
    """
    Raised when avro raise AvroTypeException

    Args:
        msg (str): Human readable string describing the exception.
        code (int): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """

    def __init__(self, msg: str, code: int) -> None:
        self.msg = msg
        self.code = code


class KafkaConsumerError(AioEventBaseException):
    """
    Raised when avro raise AvroTypeException

    Args:
        msg (str): Human readable string describing the exception.
        code (int): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """

    def __init__(self, msg: str, code: int) -> None:
        self.msg = msg
        self.code = code
