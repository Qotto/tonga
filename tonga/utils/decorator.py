#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from functools import wraps
from typing import Any, Callable
from tonga.stores.store_builder.errors import UninitializedStore

__all__ = [
    'check_initialized'
]


def check_initialized(func: Callable) -> Any:
    """ Check decorator, check is store is initialized

    Args:
        func (Callable): Function to wraps

    Raises:
        UninitializedStore: Raised when store is not initialized

    Returns:
        Any: Return func result
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if args[0].is_initialized():
            response = await func(*args, **kwargs)
            return response
        raise UninitializedStore('Uninitialized store', 500)
    return wrapper
