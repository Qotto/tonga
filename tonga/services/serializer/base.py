#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseSerializer

Base of each serializer
"""

from typing import Any

__all__ = [
    'BaseSerializer',
]


class BaseSerializer:
    """ Serializer base, all serializer must be inherit form this class
    """
    def __init__(self) -> None:
        """ BaseSerializer constructor
        """

    def encode(self, obj: Any) -> Any:
        """Encode function, abstract method

        Args:
            obj (Any):

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Any
        """
        raise NotImplementedError

    def decode(self, encoded_obj: Any) -> Any:
        """Decode function, abstract method

        Args:
            encoded_obj (Any):

        Raises:
            NotImplementedError: Abstract method

        Returns:
            Any
        """
        raise NotImplementedError
