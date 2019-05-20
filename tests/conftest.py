#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from aiokafka import TopicPartition

from aioevent.services.stores.local.memory import LocalStoreMemory
from aioevent.services.stores.globall.memory import GlobalStoreMemory

assigned_partitions = [TopicPartition('test', 2)]
last_offsets = {TopicPartition('test', 2): 0}
current_instance = 2
nb_replica = 4

local_memory_store = LocalStoreMemory(assigned_partitions=assigned_partitions,
                                      last_offsets=last_offsets, current_instance=current_instance,
                                      nb_replica=nb_replica, name='local_store_memory_test')

global_memory_store = GlobalStoreMemory(assigned_partitions=assigned_partitions, last_offsets=last_offsets,
                                        current_instance=current_instance, nb_replica=nb_replica,
                                        name='global_store_memory_test')


@pytest.fixture
def get_local_memory_store_connection():
    return local_memory_store


@pytest.fixture
def get_global_memory_store_connection():
    return global_memory_store
