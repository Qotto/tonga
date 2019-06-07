#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any, List

from tonga.models.events.base import BaseModel
from tonga.models.events.command.errors import CommandEventMissingProcessGuarantee


__all__ = [
    'BaseCommand',
]

PROCESSING_GUARANTEE: List[str] = ['at_least_once', 'at_most_once', 'exactly_once']


class BaseCommand(BaseModel):
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

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        """ Serialize dict to Command Class

        Args:
            event_data (Dict|str, Any]): Contains all Command Class attribute for return an instanced class

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError

    @classmethod
    def event_name(cls) -> str:
        """ Return Command Class name, used in serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
