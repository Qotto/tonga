#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import pytest
import uvloop
from kafka.client import KafkaClient as PyKafkaClient
from kafka.cluster import ClusterMetadata

# Serializer
from tonga.services.serializer.avro import AvroSerializer

# Stores import
from tonga.stores.local.memory import LocalStoreMemory
from tonga.stores.globall.memory import GlobalStoreMemory

# StoreBuilder import
from tonga.stores.store_builder.store_builder import StoreBuilder

# StoreRecord import
from tonga.models.records.store import StoreRecord
from tonga.models.handlers.store.store_record_handler import StoreRecordHandler

# Tonga Kafka client
from tonga.services.coordinator.kafka_client.kafka_client import KafkaClient

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

t_loop = uvloop.new_event_loop()

# Store test
test_local_memory_store = LocalStoreMemory(name='local_store_memory_test')
test_global_memory_store = GlobalStoreMemory(name='global_store_memory_test')

# Avro Serializer test
test_serializer = AvroSerializer(BASE_DIR + '/misc/schemas')
test_serializer_local_memory_store = LocalStoreMemory(name='local_store_memory_serializer_test')
test_serializer_global_memory_store = GlobalStoreMemory(name='global_store_memory_serializer_test')

# StatefulsetPartitionAssignor test
assignor_py_kafka_client = PyKafkaClient(bootstrap_servers='localhost:9092', client_id='test_client')
assignor_cluster_metadata = ClusterMetadata(bootstrap_servers='localhost:9092')

# StoreBuilder test
store_builder_serializer = AvroSerializer(BASE_DIR + '/misc/schemas')


tonga_kafka_client = KafkaClient(client_id='waiter', cur_instance=0, nb_replica=1, bootstrap_servers='localhost:9092')

test_store_builder_local_memory_store = LocalStoreMemory(name='test_store_builder_local_memory_store')
test_store_builder_global_memory_store = GlobalStoreMemory(name='test_store_builder_global_memory_store')

test_store_builder = StoreBuilder(name='test_store_builder', client=tonga_kafka_client, topic_store='test-store',
                                  serializer=store_builder_serializer, loop=t_loop, rebuild=True, event_sourcing=False,
                                  local_store=test_store_builder_local_memory_store,
                                  global_store=test_store_builder_global_memory_store)

store_record_handler = StoreRecordHandler(test_store_builder)
store_builder_serializer.register_event_handler_store_record(StoreRecord, store_record_handler)


@pytest.yield_fixture()
def event_loop():
    loop = t_loop
    yield loop


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
def get_assignor_kafka_client():
    return assignor_py_kafka_client


@pytest.fixture
def get_assignor_cluster_metadata():
    return assignor_cluster_metadata


@pytest.fixture
def get_store_builder():
    return test_store_builder
