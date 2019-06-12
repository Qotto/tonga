#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" StoreRecord class

More detail in BaseStoreRecord

"""

from typing import Dict, Any

from tonga.models.store_record.base import BaseStoreRecord

__all__ = [
    'StoreRecord'
]


class StoreRecord(BaseStoreRecord):
    """ StoreRecord class, used for create local & global store
    """
    def __init__(self, **kwargs) -> None:
        """
        StoreRecord constructor

        Args:
            **kwargs (Dict[str, Any]): Contains all BaseStoreRecord params

        Returns:
            None
        """
        super().__init__(**kwargs)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        """ Serialize dict to StoreRecord

        Args:
            event_data (Dict|str, Any]): Contains all StoreRecord Class attribute for return an instanced class

        Returns:
            cls: Instanced StoreRecord class
        """
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        """ Return store record class name, used by serializer

        Returns:
            str: store record name
        """
        return 'tonga.store.record'
