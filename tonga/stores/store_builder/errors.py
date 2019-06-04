#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'UninitializedStore',
    'CanNotInitializeStore',
    'FailToSendStoreRecord',
]


class UninitializedStore(RuntimeError):
    pass


class CanNotInitializeStore(RuntimeError):
    pass


class FailToSendStoreRecord(Exception):
    pass
