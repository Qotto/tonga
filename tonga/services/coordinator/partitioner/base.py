#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import List, Optional, Union


class BasePartitioner:
    """
    Base class for a partitioner
    """
    def __init__(self, partitions=None) -> None:
        """
        Initialize the partitioner

        Arguments:
            partitions: A list of available partitions (during startup) OPTIONAL.
        """
        if partitions is None:
            partitions = list()
        self.partitions = partitions

    def __call__(self, key: Union[str, bytes], all_partitions: List[int], available_partitions: Optional[List[int]]) \
            -> int:
        """ Takes a string key, num_partitions and available_partitions as argument and returns
        a partition to be used for the message

        Arguments:
            key (Union[str, bytes]): the key to use for partitioning.
            all_partitions (List[int]): a list of the topic's partitions.
            available_partitions (Optional[List[int]]): a list of the broker's currently available partitions(optional).

        Raises:
            NotImplementedError: Abstract def

        Returns:
            int: Partition number
        """
        raise NotImplementedError()
