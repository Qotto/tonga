#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain all StoreBuilder errors
"""

__all__ = [
    'UnknownOperationType',
    'RocksDBErrors'
]


class UnknownOperationType(ValueError):
    """UnknownOperationType

    This error was raised when operation type was unknown
    """


class RocksDBErrors(Exception):
    """UnknownOperationType

    This error was raised when operation type was unknown
    """
