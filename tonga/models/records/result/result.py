#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseResult Module

A *result* is a record containing results of the processing of a command.

Results should be sent to the **coffeemaker‑results** topic.

Results should use the *tonga.coffeemaker.results* namespace.

Commands have the same fields as records, and a few additional ones:

   - *error*: an optional field describing the error that occurred during the command processing.
     If this field is empty, it means that the command was successfully completed. The *error* field has two specific
     sub-fields:

        - *label*: a short label describing the error (a string without any white space)
        - *message*: a human-readable description of the error
"""

from typing import Dict, Any

from ..base import BaseRecord

__all__ = [
    'BaseResult'
]


class BaseResult(BaseRecord):
    """ BaseResult class, all *result* events must inherit from this class.

    Attributes:
        error (Dict[str, Any]): If event command fail, all information was store in error dict (see module docstring
            for more details)
    """
    error: Dict[str, Any]

    def __init__(self, error: Dict[str, Any] = None, **kwargs):
        """ BaseResult constructor

        Args:
            error (Dict[str, Any]): If event command fail, all information was store in error dict (see module docstring
                                    for more details)
            **kwargs (Dict[str, Any]): variable for BaseModel (see BaseModel for more details)
        """
        super().__init__(**kwargs)
        self.error = error

    @classmethod
    def event_name(cls) -> str:
        """ Return BaseEvent name, used in serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    def base_dict(self) -> Dict[str, Any]:
        """ Return base dict.

        Returns:
            Dict[str, Any]: Base dict contains (record_id, schema_version, partition_key, datetime,
                                                timestamp, correlation_id, context, error)
        """
        return {
            'record_id': self.record_id,
            'schema_version': self.schema_version,
            'partition_key': self.partition_key,
            'datetime': self.date.isoformat(),
            'timestamp': self.date.timestamp() * 1000,
            'correlation_id': self.correlation_id,
            'context': self.context,
            'error': self.error
        }

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
