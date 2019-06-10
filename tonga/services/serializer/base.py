#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Any, Tuple, Union, Type

from tonga.models.events.base import BaseModel
from tonga.models.handlers.base import BaseHandler
from tonga.models.store_record.base import BaseStoreRecord, BaseStoreRecordHandler

__all__ = [
    'BaseSerializer',
]


class BaseSerializer:
    """ Serializer base, all serializer must be inherit form this class
    """
    def __init__(self) -> None:
        """ BaseSerializer constructor
        """
        pass

    def encode(self, event: Any) -> Any:
        """Encode function, abstract method

        Args:
            event (Any):

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Any
        """
        raise NotImplementedError

    def decode(self, encoded_event: Any) -> Any:
        """Decode function, abstract method

        Args:
            encoded_event (Any):

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Any
        """
        raise NotImplementedError
