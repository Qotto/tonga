#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import random

from kafka.partitioner.hashed import murmur2

logger = logging.getLogger(__name__)


class StatefulsetPartitioner(object):
    @classmethod
    def __call__(cls, key, all_partitions, available):
        logger.debug('StatefulsetPartitioner')

        if key is None:
            return random.choice(all_partitions)

        try:
            if int(key) <= len(all_partitions):
                logger.debug(f'Selected partition = {all_partitions[int(key)]}')
                return all_partitions[int(key)]
            else:
                raise ValueError
        except (ValueError, KeyError):
            idx = murmur2(key)
            idx &= 0x7fffffff
            idx %= len(all_partitions)
            return all_partitions[idx]
