#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import uuid
import pytest

from tonga.services.coordinator.partitioner.statefulset_partitioner import StatefulsetPartitioner
from tonga.errors import OutsideInstanceNumber


def test_statefulset_partitioner_with_str_uuid_key():
    statefulset_partitioner = StatefulsetPartitioner(instance=1)

    for i in range(0, 100):
        assert statefulset_partitioner(uuid.uuid4().hex, [0, 1, 2, 3], [0, 1, 2, 3]) == 1


def test_statefulset_partitioner_bad_instance():
    statefulset_partitioner = StatefulsetPartitioner(instance=100)

    with pytest.raises(OutsideInstanceNumber):
        statefulset_partitioner.__call__(uuid.uuid4().hex, [0, 1, 2, 3], [0, 1, 2, 3])
