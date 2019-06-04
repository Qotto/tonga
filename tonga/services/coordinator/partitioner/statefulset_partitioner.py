#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging

from tonga.services.coordinator.partitioner.base import BasePartitioner
from tonga.services.coordinator.partitioner.errors import OutsideInstanceNumber

logger = logging.getLogger(__name__)


class StatefulsetPartitioner(BasePartitioner):
    _instance: int

    def __init__(self, instance, **kwargs):
        super().__init__(**kwargs)
        self._instance = instance

    def __call__(self, key, all_partitions, available):
        logger.debug('StatefulsetPartitioner')
        if self._instance <= len(all_partitions):
            return all_partitions[self._instance]
        raise OutsideInstanceNumber
