#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain BaseTransaction & BaseTransactionContext

Base class for make transaction
"""

from logging import (getLogger)
from abc import (ABCMeta, abstractmethod)

from typing import Dict, Callable, Union

from tonga.services.producer.base import BaseProducer
from tonga.models.structs.positioning import BasePositioning

__all__ = [
    'BaseTransaction',
    'BaseTransactionContext',
]


class BaseTransactionContext(metaclass=ABCMeta):
    """ BaseTransactionContext

    All TransactionContext must be inherit form this class
    """
    _offset: int
    _partition: int
    _topic: str

    @abstractmethod
    def get_committed_offsets(self) -> Dict[str, BasePositioning]:
        """ Return committed offsets in transaction

        Abstract method

        Raises:
            NotImplementedError: Abstract method

        Returns:

        """
        raise NotImplementedError


class BaseTransaction(metaclass=ABCMeta):
    """BaseTransaction

    All Transaction must be inherit form this class
    """
    _ctx: Union[BaseTransactionContext, None]
    _transactional_producer: Union[BaseProducer, None]
    _logger = getLogger('tonga')

    @abstractmethod
    def set_ctx(self, ctx: BaseTransactionContext) -> None:
        """ Set a new transaction context

        Args:
            ctx (BaseTransactionContext): BaseTransactionContext, calculated at each new message

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    def set_transactional_producer(self, transactional_producer: BaseProducer) -> None:
        """ Set a transactional producer

        Args:
            transactional_producer (BaseProducer): Transactional producer used for start transaction & send committed
                                                    offset

        Returns:
            None
        """
        raise NotImplementedError

    @abstractmethod
    def __call__(self, func: Callable):
        raise NotImplementedError
