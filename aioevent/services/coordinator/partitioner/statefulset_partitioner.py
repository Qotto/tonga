#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging

from aioevent.services.coordinator.partitioner.base import BasePartitioner

logger = logging.getLogger(__name__)


class StatefulsetPartitioner(BasePartitioner):
    instance: int = 0

    @classmethod
    def __call__(cls, key, all_partitions, available):
        logger.debug('StatefulsetPartitioner')
        if cls.instance <= len(all_partitions):
            return all_partitions[cls.instance]
        raise ValueError
