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
    'StorePartitionAlreadyAssigned',
    'StorePartitionNotAssigned',
    'StoreKeyNotFound',
    'KtableUnknownType',
    'StoreMetadataCantNotUpdated',
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


class StorePartitionAlreadyAssigned(AioEventBaseException):
    """
    Raised when partition is already assigned in store metadata

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


class StorePartitionNotAssigned(AioEventBaseException):
    """
    Raised when partition is already assigned in store metadata

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


class StoreKeyNotFound(AioEventBaseException):
    """
    Raised when key is not found in store

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


class KtableUnknownType(AioEventBaseException):
    """
    Raised when type is unknown

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


class StoreMetadataCantNotUpdated(AioEventBaseException):
    """
    Raised when developer try to update metadata with set function

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
