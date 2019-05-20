#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any, List

from aioevent.model.exceptions import StorePartitionAlreadyAssigned, StorePartitionNotAssigned

__all__ = [
    'BaseStores',
    'BaseStoreMetaData',
]


class BaseStoreMetaData(object):
    assigned_partitions: List[TopicPartition]
    current_instance: int
    nb_replica: int
    last_offsets: Dict[TopicPartition, int]

    def __init__(self, assigned_partitions: List[TopicPartition], last_offsets: Dict[TopicPartition, int],
                 current_instance: int, nb_replica: int):
        self.assigned_partitions = assigned_partitions
        self.current_instance = current_instance
        self.nb_replica = nb_replica
        self.last_offsets = last_offsets

    def update_last_offsets(self, tp: TopicPartition, offset: int) -> None:
        if tp not in self.last_offsets:
            raise StorePartitionNotAssigned(f'TopicPartition: {tp} is not assigned', 500)
        self.last_offsets[tp] = offset

    def assign_partition(self, tp: TopicPartition) -> None:
        if tp not in self.assigned_partitions:
            self.assigned_partitions.append(tp)
            self.last_offsets[tp] = 0
        else:
            raise StorePartitionAlreadyAssigned(f'TopicPartition: {tp} is already assigned', 500)

    def unassign_partition(self, tp: TopicPartition) -> None:
        if tp not in self.assigned_partitions:
            raise StorePartitionNotAssigned(f'TopicPartition: {tp} is not assigned', 500)
        else:
            self.assigned_partitions.remove(tp)
            del self.last_offsets[tp]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'assigned_partitions': [{'topic': tp.topic, 'partition': tp.partition} for tp in self.assigned_partitions],
            'current_instance': self.current_instance,
            'nb_replica': self.nb_replica,
            'last_offsets': [{'topic': tp.topic, 'partition': tp.partition, 'offsets': key} for tp, key in
                             self.last_offsets.items()]
        }

    @classmethod
    def from_dict(cls, meta_dict: Dict[str, Any]):
        assigned_partitions = [TopicPartition(tp['topic'], tp['partition']) for tp in
                               meta_dict['assigned_partitions']]
        last_offsets = {}
        for tp in meta_dict['last_offsets']:
            last_offsets[TopicPartition(tp['topic'], tp['partition'])] = tp['offsets']
        return cls(assigned_partitions=assigned_partitions, last_offsets=last_offsets,
                   current_instance=meta_dict['current_instance'], nb_replica=meta_dict['nb_replica'])


class BaseStores(object):
    def __init__(self, name: str):
        self._name = name

    def set_store_position(self, assigned_partitions: List[TopicPartition],
                           last_offsets: Dict[TopicPartition, int]) -> None:
        raise NotImplementedError

    def is_initialized(self) -> bool:
        raise NotImplementedError

    def get(self, key: str) -> bytes:
        raise NotImplementedError

    def get_all(self) -> Dict[str, bytes]:
        raise NotImplementedError

    def update_metadata_tp_offset(self, tp: TopicPartition, offset: int) -> None:
        raise NotImplementedError

    def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        raise NotImplementedError

    def get_metadata(self) -> BaseStoreMetaData:
        raise NotImplementedError

    def _update_metadata(self) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

