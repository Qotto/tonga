#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain KafkaTransactionContext & KafkaTransactionalManager

Module for make Kafka Transaction
"""

from typing import Callable, Dict

from tonga.services.coordinator.transaction.base import (BaseTransaction,
                                                         BaseTransactionContext)
from tonga.services.coordinator.transaction.errors import MissingKafkaTransactionContext
from tonga.services.producer.base import BaseProducer
from tonga.models.structs.positioning import (BasePositioning, KafkaPositioning)

__all__ = [
    'KafkaTransactionContext',
    'KafkaTransactionalManager'
]


class KafkaTransactionContext(BaseTransactionContext):
    """ KafkaTransactionContext class

    This class contain the transaction context, at each received message KafkaConsumer generate this class
    and update KafkaTransactionalManager
    """
    _group_id: str

    def __init__(self, topic: str, partition: int, offset: int, group_id: str):
        """KafkaTransactionContext constructor

        Args:
            topic (str): Kafka topic name
            partition (int): Kafka topic partition number
            offset (int): Kafka msg current offset
            group_id (str): KafkaConsumer group_id
        """
        self._offset: int = offset
        self._partition: int = partition
        self._topic: str = topic
        self._group_id: str = group_id

    def get_committed_offsets(self) -> Dict[str, BasePositioning]:
        """ Return committed offsets as TopicsPartitions dict

        Returns:
            Dict[TopicPartition, int]: committed offset
        """
        key = KafkaPositioning.make_class_assignment_key(self._topic, self._partition)
        return {key: KafkaPositioning(self._topic, self._partition, self._offset + 1)}

    @property
    def group_id(self) -> str:
        """ Return group_id

        Returns:
            str: return _group_id
        """
        return self._group_id


class KafkaTransactionalManager(BaseTransaction):
    """ KafkaTransactionalManager class

    Contains the latest KafkaTransactionContext of received message (One by topic / partition)
    """

    def __init__(self, transactional_producer: BaseProducer = None) -> None:
        """KafkaTransactionalManager constructor

        Attributes:
            transactional_producer (Union[KafkaProducer, None]): Transactional KafkaProducer used for start transaction
                                                                 & send committed offset to Kafka
        """
        self._ctx = None
        self._transactional_producer = transactional_producer

    def set_ctx(self, ctx: BaseTransactionContext) -> None:
        """ Set KafkaTransactionContext

        Args:
            ctx (BaseTransactionContext): KafkaTransactionContext, calculated at each new message

        Returns:
            None
        """
        self._ctx = ctx

    def set_transactional_producer(self, transactional_producer: BaseProducer) -> None:
        """ Set a transactional KafkaProducer

        Args:
            transactional_producer (BaseProducer): Transactional KafkaProducer used for start transaction
                                                                 & send committed offset to Kafka

        Returns:
            None
        """
        self._transactional_producer = transactional_producer

    def __call__(self, func: Callable):
        """ Decorator function, used for make transaction operation

        Args:
            func (Callable): Decorated function

        Raises:
            MissingKafkaTransactionContext: Raised when KafkaTransaction was missing

        Returns:
            bool: True if transaction  has been succeeding
        """
        self._logger.info('Init transactional function')

        async def make_transaction(*args, **kwargs):
            self._logger.info('Start transaction')

            if not self._transactional_producer.is_running():
                await self._transactional_producer.start_producer()

            if self._transactional_producer is None:
                raise MissingKafkaTransactionContext

            if self._ctx is None:
                raise MissingKafkaTransactionContext

            self._logger.debug('Committed offset : %s', self._ctx.get_committed_offsets())

            async with self._transactional_producer.init_transaction():
                await func(*args, **kwargs)
                await self._transactional_producer.end_transaction(committed_offsets=self._ctx.get_committed_offsets(),
                                                                   group_id=self._ctx.group_id)

            self._logger.info('End transaction')
            return True

        return make_transaction
