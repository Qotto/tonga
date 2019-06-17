#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from enum import Enum


__all__ = [
    'StoreRecordType'
]


class StoreRecordType(Enum):
    """ KafkaPositioning class

    Class manage kafka positioning (like TopicsPartition)
    """
    SET: int = 0
    DEL: int = 1
