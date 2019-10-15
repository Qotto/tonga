#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseConsumer Class

Base of all consumer class
"""

from abc import ABCMeta, abstractmethod

from typing import Dict

from tonga.models.structs.positioning import BasePositioning

__all__ = [
    'BaseConsumer',
]


class BaseConsumer(metaclass=ABCMeta):
    """BaseConsumer all consumer must be inherit from this class
    """

    @abstractmethod
    async def start_consumer(self) -> None:
        """Start consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def stop_consumer(self) -> None:
        """Stop consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    def is_running(self) -> bool:
        """Return is_running flag from consumer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            Bool: true if consumer is running otherwise false
        """
        raise NotImplementedError

    @abstractmethod
    async def get_last_committed_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get last committed offsets

        Returns:
            Dict[str, BasePositioning]: Contains all assigned partitions with last committed offsets
        """
        raise NotImplementedError

    @abstractmethod
    async def get_current_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get current offsets

        Returns:
            Dict[str, BasePositioning]: Contains all assigned partitions with current offsets
        """
        raise NotImplementedError

    @abstractmethod
    async def get_beginning_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get beginning offsets

        Returns:
            Dict[str, BasePositioning]: Contains all assigned partitions with beginning offsets
        """
        raise NotImplementedError

    @abstractmethod
    async def get_last_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get last offsets

        Returns:
            Dict[str, BasePositioning]: Contains all assigned partitions with last offsets
        """
        raise NotImplementedError

    @abstractmethod
    async def load_offsets(self, mod: str = 'earliest') -> None:
        """
        This method was call before consume topics, assign position to consumer

        Args:
            mod: Start position of consumer (earliest, latest, committed)

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def listen_records(self, mod: str = 'latest') -> None:
        """Listen all event on assigned topic

        Args:
            mod (str): starting point of consumer, possible value (latest, earliest, committed)

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def check_if_store_is_ready(self) -> None:
        """ If store is ready consumer set store initialize flag to true

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
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

    @abstractmethod
    async def seek_to_beginning(self, positioning: BasePositioning = None) -> None:
        """
        Seek to fist offset, mod 'earliest'

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number)

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def seek_to_end(self, positioning: BasePositioning = None) -> None:
        """
        Seek to latest offset, mod 'latest'

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number)

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def seek_to_last_commit(self, positioning: BasePositioning = None) -> None:
        """
        Seek to last committed offsets, mod 'committed'

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number)

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    async def seek_custom(self, positioning: BasePositioning) -> None:
        """
        Seek to custom offsets

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number / offset number)

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    def get_offset_bundle(self) -> Dict[str, Dict[str, BasePositioning]]:
        """
        Return a bundle with each assigned assigned topic/partition with current, latest, last committed
        topic/partition as dict

        Returns:
            Dict[str, Dict[TopicPartition, int]]: Contains current_offset / last_offset / last_committed_offset
        """
        raise NotImplementedError

    @abstractmethod
    def get_current_offset(self) -> Dict[str, BasePositioning]:
        """
        Return current offset of each assigned topic/partition

        Returns:
            Dict[str, BasePositioning]: Dict contains current offset of each assigned partition
        """
        raise NotImplementedError

    @abstractmethod
    def get_last_offset(self) -> Dict[str, BasePositioning]:
        """
        Return last offset of each assigned topic/partition

        Returns:
            Dict[str, BasePositioning]: Dict contains latest offset of each assigned partition
        """
        raise NotImplementedError

    @abstractmethod
    def get_last_committed_offset(self) -> Dict[str, BasePositioning]:
        """
        Return last committed offset of each assigned topic/partition

        Returns:
            Dict[str, BasePositioning]: Dict contains last committed offset of each assigned partition
        """
        raise NotImplementedError
