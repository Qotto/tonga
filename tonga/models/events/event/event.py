#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseEvent module

A *event* is a record to describes what happened in the system.
"""

from typing import Dict, Any

from tonga.models import BaseModel


class BaseEvent(BaseModel):
    """BaseEvent class, all *classic* events must inherit from this class.
    """
    def __init__(self, **kwargs):
        """BaseEvent constructor

        Args:
            **kwargs (Dict[str, Any]): see BaseModel class
        """
        super().__init__(**kwargs)

    @classmethod
    def event_name(cls) -> str:
        """ Return BaseEvent name, used in serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        """ Serialize dict to Event Class

        Args:
            event_data (Dict|str, Any]): Contains all Event class attribute for return an instanced class

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
