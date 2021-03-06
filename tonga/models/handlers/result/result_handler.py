#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

"""BaseResultHandler

All result handler must be inherit from this class. On_result function was called by consumer on each received result.

For make an transaction in handle function return 'transaction' as string after end transaction otherwise return none.
"""

from typing import Union

from tonga.models.handlers.base import BaseHandler
from tonga.models.records.result.result import BaseResult

__all__ = [
    'BaseResultHandler',
]


class BaseResultHandler(BaseHandler):
    """ Base of all result handler
    """

    @classmethod
    def handler_name(cls) -> str:
        """ Return handler name, used by serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    async def on_result(self, event: BaseResult) -> Union[str, None]:
        """ This function is automatically call by Tonga when an result with same name was receive by consumer

        Args:
            event (BaseResult): Result event receive by consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
