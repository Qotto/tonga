#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import uuid
import pytest
from kafka.partitioner.hashed import murmur2

from aioevent.services.coordinator.partitioner.key_partitioner import KeyPartitioner


def get_good_partition(key, all_partitions):
    if isinstance(key, str):
        key = bytes(key, 'utf-8')
    idx = murmur2(key)
    idx &= 0x7fffffff
    idx %= len(all_partitions)
    return all_partitions[idx]


def test_key_partitioner_with_str_uuid_key():
    for i in range(0, 100):
        test_uuid = uuid.uuid4().hex
        assert KeyPartitioner.__call__(test_uuid, [0, 1, 2, 3], [0, 1, 2, 3]) == get_good_partition(test_uuid,
                                                                                                    [0, 1, 2, 3])


def test_key_partitioner_with_bytes_uuid_key():
    for i in range(0, 100):
        test_uuid = uuid.uuid4().hex
        assert KeyPartitioner.__call__(bytes(test_uuid, 'utf-8'), [0, 1, 2, 3],
                                       [0, 1, 2, 3]) == get_good_partition(bytes(test_uuid, 'utf-8'), [0, 1, 2, 3])


def test_key_partitioner_bad_format():
    with pytest.raises(ValueError):
        KeyPartitioner.__call__(['rofl'], [0, 1, 2, 3], [0, 1, 2, 3])


def test_key_partitioner_missing_key():
    r = KeyPartitioner.__call__(None, [0, 1, 2, 3], [0, 1, 2, 3])
    assert r in range(0, 4)
