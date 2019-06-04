#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from functools import wraps
from tonga.stores.store_builder.errors import UninitializedStore

__all__ = [
    'check_initialized'
]


def check_initialized(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if args[0].is_initialized():
            response = await func(*args, **kwargs)
            return response
        else:
            raise UninitializedStore('Uninitialized store', 500)
    return wrapper
