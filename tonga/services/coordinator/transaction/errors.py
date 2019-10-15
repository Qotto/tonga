#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Transaction errors

Contain all transaction errors
"""

__all__ = [
    'MissingKafkaTransactionContext'
]


class MissingKafkaTransactionContext(ValueError):
    """ This errors was raised when KafkaTransactionContext missing in TransactionManager
    """
