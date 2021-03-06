#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

"""BaseEventHandler

All event handler must be inherit from this class. Handle function was called by consumer on each received events.

For make an transaction in handle function return 'transaction' as string after end transaction otherwise return none.
"""

from typing import Union, Type

from tonga.models.handlers.base import BaseHandler
from tonga.models.records.event.event import BaseEvent

__all__ = [
    'BaseEventHandler'
]


class BaseEventHandler(BaseHandler):
    """ Base of all event handler
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

    async def handle(self, event: Type[BaseEvent]) -> Union[str, None]:
        """ This function is automatically call by Tonga when event with same name was receive by consumer

        Args:
            event (BaseEvent):  Event receive by consumer

        Notes:
            If handle make an transaction return 'transaction' as string at transaction end

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
