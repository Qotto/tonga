#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain BaseStores / BaseStoreMetaData

All store must be inherit form BaseStore

BaseStoreMetaData is used for store positioning (Assigned topics partitions / kafka offset), each store contain
an BaseStoreMetaData but this one was never send in Kafka. Used by store or developer purpose
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, Any, List

from tonga.models.structs.positioning import BasePositioning

__all__ = [
    'BaseStoreMetaData',
]


class BaseStoreMetaData(metaclass=ABCMeta):
    """ Store positioning class
    """
    assigned_partitions: List[BasePositioning]
    current_instance: int
    nb_replica: int
    last_offsets: Dict[str, BasePositioning]

    @abstractmethod
    def update_last_offsets(self, positioning: BasePositioning) -> None:
        raise NotImplementedError

    @abstractmethod
    def assign_partition(self, positioning: BasePositioning) -> None:
        raise NotImplementedError

    @abstractmethod
    def unassign_partition(self, positioning: BasePositioning) -> None:
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """ Return class as dict

        Returns:
            Dict[str, Any]: class as dict format
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_dict(cls, meta_dict: Dict[str, Any]):
        """ Return class form dict

        Args:
            meta_dict (Dict[str, Any]): Metadata class as dict format

        Returns:
            BaseStoreMetaData: return instanced class
        """
        raise NotImplementedError
