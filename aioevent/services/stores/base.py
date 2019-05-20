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
    current_instance = int
    nb_replica = int
    last_offsets: Dict[TopicPartition: int]

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

    @classmethod
    def from_dict(cls, meta_dict: Dict[str, Any]):
        return cls(**meta_dict)


class BaseStores(object):
    def __init__(self, name: str):
        self._name = name

    def get(self, key: bytes) -> Any:
        raise NotImplementedError

    def get_all(self) -> Dict[bytes, Any]:
        raise NotImplementedError

    def update_metadata_tp_offset(self, tp: TopicPartition, offset: int) -> None:
        raise NotImplementedError

    def _get_metadata(self) -> BaseStoreMetaData:
        raise NotImplementedError

    def _update_metadata(self) -> None:
        raise NotImplementedError