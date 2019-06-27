#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

"""
Contains StoreRecordType class, used by StoreBuilder for set operation_type (StoreRecord)
"""

from enum import Enum


__all__ = [
    'StoreRecordType'
]


class StoreRecordType(Enum):
    """ StoreRecordType

    Used by StoreBuilder as *operating_type* for *set* or *del* key & value in store

    Attributes:
        SET (str): StoreRecord operation type (created or updated)
        DEL (str): StoreRecord operation type (deleted)
    """
    SET: str = 'set'
    DEL: str = 'del'
