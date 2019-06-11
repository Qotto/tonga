#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import random
from typing import Union, List, Optional

from kafka.partitioner.hashed import murmur2

from tonga.services.coordinator.partitioner.base import BasePartitioner
from tonga.services.coordinator.partitioner.errors import BadKeyType


class KeyPartitioner(BasePartitioner):
    """KeyPartitioner

    Send event in topic partition with murmur2 algo
    """

    def __call__(self, key: Union[str, bytes], all_partitions: List[int], available_partitions: Optional[List[int]]) \
            -> int:
        """
        Returns a partition to be used for the message

        Args:
            key (Union[str, bytes]): the key to use for partitioning.
            all_partitions (List[int]): a list of the topic's partitions.
            available_partitions (Optional[List[int]]): a list of the broker's currently available partitions(optional).

        Raises:
            BadKeyType: If key is not bytes serializable

        Returns:
            int: Partition number
        """
        if key is None:
            return random.choice(all_partitions)

        if isinstance(key, str):
            key = bytes(key, 'utf-8')
        if isinstance(key, bytes):
            idx = murmur2(key)
            idx &= 0x7fffffff
            idx %= len(all_partitions)
            return all_partitions[idx]
        raise BadKeyType
