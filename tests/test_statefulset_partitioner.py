#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import uuid
import pytest

from aioevent.services.coordinator.partitioner.statefulset_partitioner import StatefulsetPartitioner


def test_statefulset_partitioner_with_str_uuid_key():
    statefulset_partitioner = StatefulsetPartitioner
    setattr(statefulset_partitioner, 'instance', 1)

    for i in range(0, 100):
        assert statefulset_partitioner.__call__(uuid.uuid4().hex, [0, 1, 2, 3], [0, 1, 2, 3]) == 1


def test_statefulset_partitioner_bad_instance():
    statefulset_partitioner = StatefulsetPartitioner
    setattr(statefulset_partitioner, 'instance', 100)

    with pytest.raises(ValueError):
        statefulset_partitioner.__call__(uuid.uuid4().hex, [0, 1, 2, 3], [0, 1, 2, 3])
