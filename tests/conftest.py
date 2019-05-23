#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import pytest
import asyncio
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata

# Serializer
from aioevent.services.serializer.avro import AvroSerializer

# Stores import
from aioevent.stores.local.memory import LocalStoreMemory
from aioevent.stores.globall.memory import GlobalStoreMemory

# StoreBuilder import
from aioevent.stores.store_builder.store_builder import StoreBuilder

# StoreRecord import
from aioevent.models.store_record.store_record import StoreRecord
from aioevent.models.store_record.store_record_handler import StoreRecordHandler

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# Store test
test_local_memory_store = LocalStoreMemory(name='local_store_memory_test')
test_global_memory_store = GlobalStoreMemory(name='global_store_memory_test')

# Avro Serializer test
test_serializer = AvroSerializer(BASE_DIR + '/misc/schemas')
test_serializer_local_memory_store = LocalStoreMemory(name='local_store_memory_serializer_test')
test_serializer_global_memory_store = GlobalStoreMemory(name='global_store_memory_serializer_test')

# StoreBuilder test
store_builder_serializer = AvroSerializer(BASE_DIR + '/misc/schemas')

admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='test_store_builder')
cluster_metadata = ClusterMetadata(bootstrap_servers='localhost:9092')
loop = asyncio.get_event_loop()
test_store_builder_local_memory_store = LocalStoreMemory(name='test_store_builder_local_memory_store')
test_store_builder_global_memory_store = GlobalStoreMemory(name='test_store_builder_global_memory_store')
test_store_builder = StoreBuilder('test_store_builder', 0, 1, 'test-store', store_builder_serializer,
                                  test_store_builder_local_memory_store, test_store_builder_global_memory_store,
                                  'localhost:9092', cluster_metadata, admin_client, loop, False, False)
store_record_handler = StoreRecordHandler(test_store_builder)
store_builder_serializer.register_event_handler_store_record(StoreRecord, store_record_handler)


@pytest.fixture
def get_local_memory_store_connection():
    return test_local_memory_store


@pytest.fixture
def get_global_memory_store_connection():
    return test_global_memory_store


@pytest.fixture
def get_avro_serializer():
    return test_serializer


@pytest.fixture
def get_avro_serializer_store():
    return test_serializer_local_memory_store, test_serializer_global_memory_store


@pytest.fixture
def get_store_builder():
    return test_store_builder
