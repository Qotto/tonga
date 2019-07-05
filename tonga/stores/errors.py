#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contains all store errors
"""

__all__ = [
    'StoreKeyNotFound',
    'StorePartitionAlreadyAssigned',
    'StorePartitionNotAssigned',
    'BadEntryType'
]


class StoreKeyNotFound(Exception):
    """StoreKeyNotFound

    This error was raised when store not found value by key
    """


class BadEntryType(TypeError):
    """StoreKeyNotFound

    This error was raised when store not found value by key
    """


class StorePartitionAlreadyAssigned(Exception):
    """StorePartitionAlreadyAssigned

    This error was raised when store is already assigned on topic
    """


class StorePartitionNotAssigned(Exception):
    """StorePartitionNotAssigned

    This error was raised when store have not assigned partition
    """
