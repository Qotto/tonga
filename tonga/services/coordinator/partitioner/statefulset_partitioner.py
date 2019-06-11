#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Union, List, Optional

from tonga.services.coordinator.partitioner.base import BasePartitioner
from tonga.services.coordinator.partitioner.errors import OutsideInstanceNumber


class StatefulsetPartitioner(BasePartitioner):
    """StatefulsetPartitioner

    Send event in topic partition with current instance

    Attributes:
        _instance (int): current instance
    """
    _instance: int

    def __init__(self, instance: int, **kwargs):
        """ StatefulsetPartitioner constructor, initialize the partitioner

        Args:
            instance (int): current instance as integer
            **kwargs (Dict[str, Any]): BasePartitioner params
        """
        super().__init__(**kwargs)
        self._instance = instance

    def __call__(self, key: Union[str, bytes], all_partitions: List[int], available_partitions: Optional[List[int]]) \
            -> int:
        """
        Returns a partition to be used for the message

        Args:
            key (Union[str, bytes]): the key to use for partitioning.
            all_partitions (List[int]): a list of the topic's partitions.
            available_partitions (Optional[List[int]]): a list of the broker's currently available partitions(optional).

        Raises:
            OutsideInstanceNumber: If instance is out of range

        Returns:
            int: Partition number
        """
        if self._instance <= len(all_partitions):
            return all_partitions[self._instance]
        raise OutsideInstanceNumber
