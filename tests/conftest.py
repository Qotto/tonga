#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest
import os

# Serializer
from aioevent.services.serializer.avro import AvroSerializer

# Stores import
from aioevent.stores.local.memory import LocalStoreMemory
from aioevent.stores.globall.memory import GlobalStoreMemory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


test_local_memory_store = LocalStoreMemory(name='local_store_memory_test')

test_global_memory_store = GlobalStoreMemory(name='global_store_memory_test')

serializer = AvroSerializer(BASE_DIR + '/misc/schemas')
test_serializer_local_memory_store = LocalStoreMemory(name='local_store_memory_serializer_test')
test_serializer_global_memory_store = GlobalStoreMemory(name='global_store_memory_serializer_test')


@pytest.fixture
def get_local_memory_store_connection():
    return test_local_memory_store


@pytest.fixture
def get_global_memory_store_connection():
    return test_global_memory_store


@pytest.fixture
def get_avro_serializer():
    return serializer


@pytest.fixture
def get_avro_serializer_store():
    return test_serializer_local_memory_store, test_serializer_global_memory_store
