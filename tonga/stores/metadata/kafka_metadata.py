#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain StoreMetaData

StoreMetaData is used for store positioning (Assigned topics partitions / kafka offset), each store contain
an StoreMetaData but this one was never send in Kafka. Used by store or developer purpose
"""

from typing import Dict, Any, List


from tonga.models.structs.positioning import (BasePositioning, KafkaPositioning)
from tonga.stores.metadata.base import BaseStoreMetaData
from tonga.stores.errors import StorePartitionAlreadyAssigned, StorePartitionNotAssigned

__all__ = [
    'KafkaStoreMetaData',
]


class KafkaStoreMetaData(BaseStoreMetaData):
    """ Store positioning class

    Attributes:
        assigned_partitions (List[BasePositioning]): List of assigned partition
        current_instance (int): Project current instance
        nb_replica (int): Number of project replica
        last_offsets (Dict[Tuple, BasePositioning]): List of last offsets consumed by store
    """

    def __init__(self, assigned_partitions: List[BasePositioning],
                 last_offsets: Dict[str, BasePositioning],
                 current_instance: int, nb_replica: int) -> None:
        """ BaseStoreMetaData constructor

        Args:
            assigned_partitions (List[BasePositioning]): List of assigned partition
            current_instance (int): Project current instance
            nb_replica (int): Number of project replica
            last_offsets (Dict[str, BasePositioning]): List of last offsets consumed by store

        Returns:
            None
        """
        self.assigned_partitions = assigned_partitions
        self.current_instance = current_instance
        self.nb_replica = nb_replica
        self.last_offsets = last_offsets

    def update_last_offsets(self, positioning: BasePositioning) -> None:
        """ Update last offsets by TopicPartition

        Args:
            positioning (BasePositioning): Class contains topic name / current partition / current offset

        Raises:
            StorePartitionNotAssigned: raised when partition was not assigned

        Returns:
            None
        """
        if positioning.make_assignment_key() not in self.last_offsets:
            raise StorePartitionNotAssigned
        self.last_offsets[positioning.make_assignment_key()] = positioning

    def assign_partition(self,  positioning: BasePositioning) -> None:
        """Assign partition by TopicPartition

        Args:
            positioning (BasePositioning): Class contains topic name / current partition / current offset

        Raises:
            StorePartitionAlreadyAssigned: raised when partition is already assigned

        Returns:
            None
        """
        if positioning.make_assignment_key() not in self.assigned_partitions:
            self.assigned_partitions.append(positioning)
            self.last_offsets[positioning.make_assignment_key()] = positioning
        else:
            raise StorePartitionAlreadyAssigned

    def unassign_partition(self, positioning: BasePositioning) -> None:
        """Unassign partition by TopicPartition

        Args:
            positioning (BasePositioning): Class contains topic name / current partition / current offset

        Raises:
            StorePartitionNotAssigned: raised when partition was not assigned

        Returns:
            None
        """
        if positioning in self.assigned_partitions:
            self.assigned_partitions.remove(positioning)
            del self.last_offsets[positioning.make_assignment_key()]
        raise StorePartitionNotAssigned

    def to_dict(self) -> Dict[str, Any]:
        """ Return class as dict

        Returns:
            Dict[str, Any]: class as dict format
        """
        return {
            'assigned_partitions': [{'topic': positioning.get_topics(), 'partition': positioning.get_partition()}
                                    for positioning in self.assigned_partitions],
            'current_instance': self.current_instance,
            'nb_replica': self.nb_replica,
            'last_offsets': [{'topic': positioning.get_topics(), 'partition': positioning.get_partition(),
                              'offsets': positioning.get_current_offset()} for key, positioning
                             in self.last_offsets.items()]
        }

    @classmethod
    def from_dict(cls, meta_dict: Dict[str, Any]):
        """ Return class form dict

        Args:
            meta_dict (Dict[str, Any]): Metadata class as dict format

        Returns:
            BaseStoreMetaData: return instanced class
        """
        assigned_partitions: List[BasePositioning] = [KafkaPositioning(positioning['topic'],
                                                                       positioning['partition'], 0) for positioning in
                                                      meta_dict['assigned_partitions']]
        last_offsets: Dict[str, BasePositioning] = dict()
        for dict_positioning in meta_dict['last_offsets']:
            key = KafkaPositioning.make_class_assignment_key(dict_positioning['topic'],  dict_positioning['partition'])
            last_offsets[key] = KafkaPositioning(dict_positioning['topic'],
                                                 dict_positioning['partition'],
                                                 dict_positioning['offsets'])
        return cls(assigned_partitions=assigned_partitions, last_offsets=last_offsets,
                   current_instance=meta_dict['current_instance'], nb_replica=meta_dict['nb_replica'])
