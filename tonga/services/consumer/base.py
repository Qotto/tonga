#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseConsumer Class

Base of all consumer class
"""

from typing import List


__all__ = [
    'BaseConsumer',
]


class BaseConsumer:
    """BaseConsumer all consumer must be inherit from this class
    """

    def __init__(self) -> None:
        """BaseConsumer constructor
        """

    async def start_consumer(self) -> None:
        """Start consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    async def stop_consumer(self) -> None:
        """Stop consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    def is_running(self) -> bool:
        """Return is_running flag from consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            Bool: true if consumer is running otherwise false
        """
        raise NotImplementedError

    async def listen_event(self, mod: str = 'latest') -> None:
        """Listen all event on assigned topic

        Args:
            mod (str): starting point of consumer, possible value (latest, earliest, committed)

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    async def listen_store_records(self, rebuild: bool = False) -> None:
        """Listen all event on assigned topic

        Args:
            rebuild (str): if rebuild is set to true, consumer was on earliest position otherwise if rebuild is False,
                           consumer start on last offset (see StoreMetadata for more details)

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    async def get_many(self, partitions: List[int] = None, max_records: int = None):
        """ Return many massage in a batch

        Args:
            partitions (List[int]): Contains all desired partitions
            max_records (int): Maximum number of messages

        Raises:
            NotImplementedError: Abstract def

        Returns:
            batch or list[msg], depend of your implementation
        """
        raise NotImplementedError

    async def get_one(self, partitions: List[int] = None):
        """ Return one message

        Args:
            partitions (List[int]): Contains all desired partitions

        Raises:
            NotImplementedError: Abstract def

        Returns:
            BaseModel, depend of your implementation
        """
        raise NotImplementedError
