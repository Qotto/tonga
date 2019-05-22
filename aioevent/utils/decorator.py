#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from functools import wraps
from aioevent.models.exceptions import UninitializedStore

__all__ = [
    'check_initialized'
]


def check_initialized(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args[0].is_initialized():
            return func(*args, **kwargs)
        else:
            raise UninitializedStore('Uninitialized store', 500)
    return wrapper
