#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" All store record errors
"""

__all__ = [
    'UnknownStoreRecordType',
]


class UnknownStoreRecordType(Exception):
    """UnknownStoreRecordType

    This error was raised when store record type as been unknown
    """
