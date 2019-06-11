#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'UninitializedStore',
    'CanNotInitializeStore',
    'FailToSendStoreRecord',
]


class UninitializedStore(RuntimeError):
    """UninitializedStore

    This error was raised when store is not initialized
    """
    pass


class CanNotInitializeStore(RuntimeError):
    """CanNotInitializeStore

    This error was raised when StoreBuilder can't initialize store
    """
    pass


class FailToSendStoreRecord(Exception):
    """FailToSendStoreRecord

    This error was raised when StoreBuilder fail to send StoreRecord
    """
    pass
