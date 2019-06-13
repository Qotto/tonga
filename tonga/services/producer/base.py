#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseProducer class

All producer must be inherit form this class
"""

from typing import Dict

from aiokafka import TopicPartition
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.producer.producer import TransactionContext

from tonga.models.records.base import BaseRecord

__all__ = [
    'BaseProducer',
]


class BaseProducer:
    """ BaseProducer all producer must be inherit form this class
    """
    def __init__(self) -> None:
        """ BaseProducer constructor
        """

    async def start_producer(self) -> None:
        """
        Start producer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    async def stop_producer(self) -> None:
        """
        Stop producer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    def is_running(self) -> bool:
        """
        Get is running

        Raises:
            NotImplementedError: Abstract def

        Returns:
            bool
        """
        raise NotImplementedError

    async def send_and_await(self, event: BaseRecord, topic: str) -> None:
        """
        Send a message and await an acknowledgments

        Args:
            event (BaseModel): Event
            topic (str): topics name

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    def create_batch(self) -> BatchBuilder:
        """
        Creates an empty batch

        Raises:
            NotImplementedError: Abstract def

        Returns:
            BatchBuilder
        """
        raise NotImplementedError

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        """
        Sends batch

        Args:
            batch (BatchBuilder): Batch
            topic (str): topic name
            partition (int): partition number

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    def init_transaction(self) -> TransactionContext:
        """
        Sugar function, inits transaction

        Raises:
            NotImplementedError: Abstract def

        Returns:
            TransactionContext
        """
        raise NotImplementedError

    async def end_transaction(self, committed_offsets: Dict[TopicPartition, int], group_id: str) -> None:
        """
        Sugar function, ends transaction

        Args:
            committed_offsets (Dict[TopicPartition, int]): Committed offsets during transaction
            group_id (str): Group_id to commit

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
