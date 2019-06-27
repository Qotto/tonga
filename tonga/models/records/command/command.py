#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseCommand Module

A *command* is a record containing instructions to be processed by a service.
The result of this processing is notified in a *result* record.

Commands, whenever possible, should be idempotent_. It means that a command applied twice should have the same effect
as a command applied once.

Idempotence is not always possible. For instance, sending an e-mail cannot be idempotent: sending an e-mail once is
not the same as sending it twice. Even if we try to deduplicate commands, there is no way, with usual SMTP servers,
to send an e-mail and acknowledge the command in the same transaction.

Commands have the same fields as records, and a few additional ones:

processing_guarantee: one of
   - *at_least_once*: Should be processed at least once. It is tolerated, in case of a failure, for it to be processed
     multiple times.
   - *at_most_once*: Should be processed at most once. It is tolerated, in case of a failure, for it to not be processed
     at all.
   - *exactly_once*: Should be processed exactly once. It is not tolerated, in case of a failure, for it to be processed
     more or less than once.

.. _idempotent:
   https://en.wikipedia.org/wiki/Idempotence
"""

from typing import Dict, Any, List

from tonga.models.records.base import BaseRecord
from tonga.models.records.command.errors import CommandEventMissingProcessGuarantee


__all__ = [
    'BaseCommand',
]

PROCESSING_GUARANTEE: List[str] = ['at_least_once', 'at_most_once', 'exactly_once']


class BaseCommand(BaseRecord):
    """BaseCommand class, all event command must inherit from this class.

    Attributes:
        processing_guarantee (str): see class description for more details
    """
    processing_guarantee: str

    def __init__(self, processing_guarantee: str = None, **kwargs):
        """BaseCommand constructor

        Args:
            processing_guarantee (str): see class description for more details
            **kwargs (Dict[str, Any]): see BaseModel class
        """
        super().__init__(**kwargs)
        if processing_guarantee in PROCESSING_GUARANTEE:
            self.processing_guarantee = processing_guarantee
        else:
            raise CommandEventMissingProcessGuarantee

    def base_dict(self) -> Dict[str, Any]:
        """ Return base dict.

        Returns:
            Dict[str, Any]: Base dict contains (record_id, schema_version, partition_key, datetime,
                                                timestamp, correlation_id, context, processing_guarantee)
        """
        return {
            'record_id': self.record_id,
            'schema_version': self.schema_version,
            'partition_key': self.partition_key,
            'datetime': self.date.isoformat(),
            'timestamp': self.date.timestamp() * 1000,
            'correlation_id': self.correlation_id,
            'context': self.context,
            'processing_guarantee': self.processing_guarantee
        }

    @classmethod
    def event_name(cls) -> str:
        """ Return Command Class name, used in serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        """ Serialize BaseRecord to dict

        Raises:
            NotImplementedError: Abstract def

        Returns:
            Dict[str, Any]: class in dict format
        """
        raise NotImplementedError

    @classmethod
    def from_dict(cls, dict_data: Dict[str, Any]):
        """ Deserialize dict to BaseRecord

        Args:
            dict_data (Dict|str, Any]): Contains all BaseRecord Class attribute for return an instanced class

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
