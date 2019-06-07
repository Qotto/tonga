#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from tonga.models.handlers.base import BaseHandler
from tonga.models.events.event.event import BaseEvent

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

    async def handle(self, event: BaseEvent, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        """ This function is automatically call by Tonga when event with same name was receive by consumer

        Args:
            event (BaseEvent):  Event receive by consumer
            tp (TopicPartition): NamedTuple with topic name & partition number (more information in kafka-python
                                 or aiokafka
            group_id (str): Consumer group id, useful for make transaction in handler
            offset (int): Offset of receive message (used for commit transaction)

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
