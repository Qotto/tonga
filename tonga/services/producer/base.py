#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseProducer class

All producer must be inherit form this class
"""

from abc import ABCMeta, abstractmethod
from typing import Union, Awaitable, List, Dict

from tonga.models.records.base import BaseRecord
from tonga.models.store.store_record import StoreRecord
from tonga.models.structs.positioning import BasePositioning

__all__ = [
    'BaseProducer',
]


class BaseProducer(metaclass=ABCMeta):
    """ BaseProducer all producer must be inherit form this class
    """

    @abstractmethod
    async def start_producer(self) -> None:
        """
        Start producer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def stop_producer(self) -> None:
        """
        Stop producer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    def is_running(self) -> bool:
        """
        Get is running

        Raises:
            NotImplementedError: Abstract def

        Returns:
            bool
        """
        raise NotImplementedError

    @abstractmethod
    async def send_and_wait(self, msg: Union[BaseRecord, StoreRecord], topic: str) -> BasePositioning:
        """
        Send a message and await an acknowledgments

        Args:
            msg (Union[BaseRecord, StoreRecord]): Event
            topic (str): topics name

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def send(self, msg: Union[BaseRecord, StoreRecord], topic: str) -> Awaitable:
        """
        Send a message and await an acknowledgments

        Args:
            msg (Union[BaseRecord, StoreRecord]: Event to send in Kafka, inherit form BaseRecord
            topic (str): Topic name to send massage

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def partitions_by_topic(self, topic: str) -> List[int]:
        """
        Get partitions by topic name

        Args:
            topic (str): topic name

        Returns:
            List[int]: list of partitions
        """
        raise NotImplementedError

    @abstractmethod
    def init_transaction(self):
        """
        Sugar function, inits transaction

        Raises:
            NotImplementedError: Abstract def
        """
        raise NotImplementedError

    @abstractmethod
    async def end_transaction(self, committed_offsets: Dict[str, BasePositioning], group_id: str) -> None:
        """
        Ends transaction

        Args:
            committed_offsets (Dict[str, BasePositioning]): Committed offsets during transaction
            group_id (str): Group_id to commit

        Returns:
            None
        """
        raise NotImplementedError
