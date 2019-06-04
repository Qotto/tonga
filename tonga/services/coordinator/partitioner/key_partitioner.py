#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import random

from kafka.partitioner.hashed import murmur2

from tonga.services.coordinator.partitioner.base import BasePartitioner
from tonga.services.coordinator.partitioner.errors import BadKeyType

logger = logging.getLogger(__name__)


class KeyPartitioner(BasePartitioner):
    def __call__(self, key, all_partitions, available):
        logger.debug('KeyPartitioner')

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
