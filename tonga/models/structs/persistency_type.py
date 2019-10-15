#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

"""
Contains DBType class, used by Store choose persistency store
"""

from enum import Enum


__all__ = [
    'PersistencyType'
]


class PersistencyType(Enum):
    """ PersistencyType

    Used by Store as *persistency_type* for choose persistency layer

    Attributes:
        MEMORY (str): Memory persistency
        SHELVE (str): Shelve persistency
        ROCKSDB (str): RocksDB persistency
    """
    MEMORY: str = 'MEMORY'
    SHELVE: str = 'SHELVE'
    ROCKSDB: str = 'ROCKSDB'
