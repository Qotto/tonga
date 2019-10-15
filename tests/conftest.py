#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os

import pytest
import uvloop
from kafka.client import KafkaClient as PyKafkaClient
from kafka.cluster import ClusterMetadata

# StoreRecord import
from tonga.models.store.store_record import StoreRecord
from tonga.models.store.store_record_handler import StoreRecordHandler
# PersistencyType import
from tonga.models.structs.persistency_type import PersistencyType
# Tonga Kafka client
from tonga.services.coordinator.client.kafka_client import KafkaClient
# Serializer
from tonga.services.serializer.avro import AvroSerializer
from tonga.stores.global_store import GlobalStore
# Local & global store import
from tonga.stores.local_store import LocalStore
# KafkaStoreManager import
from tonga.stores.manager.kafka_store_manager import KafkaStoreManager
# Persistency import
from tonga.stores.persistency.memory import MemoryPersistency
from tonga.stores.persistency.rocksdb import RocksDBPersistency
from tonga.stores.persistency.shelve import ShelvePersistency

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

t_loop = uvloop.new_event_loop()

# Create persistency test
test_memory_persistency = MemoryPersistency()
test_memory_persistency.__getattribute__('_set_initialize').__call__()

test_shelve_persistency = ShelvePersistency(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                         'local_store.db'))
test_shelve_persistency.__getattribute__('_set_initialize').__call__()

test_rocksdb_persistency = RocksDBPersistency(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                           'local_store'))
test_rocksdb_persistency.__getattribute__('_set_initialize').__call__()

# Local Store & global store with memory persistency test
test_local_store_memory_persistency = LocalStore(db_type=PersistencyType.MEMORY, loop=t_loop)
test_global_store_memory_persistency = GlobalStore(db_type=PersistencyType.MEMORY)

# Avro Serializer test
test_serializer = AvroSerializer(BASE_DIR + '/misc/schemas')
test_serializer_local_store_memory_persistency = LocalStore(db_type=PersistencyType.MEMORY, loop=t_loop)
test_serializer_global_store_memory_persistency = GlobalStore(db_type=PersistencyType.MEMORY)

# StoreBuilder test
store_builder_serializer = AvroSerializer(BASE_DIR + '/misc/schemas')

# tonga_kafka_client = KafkaClient(client_id='waiter', cur_instance=0, nb_replica=1, bootstrap_servers='localhost:9092')

# test_store_manager = KafkaStoreManager(client=tonga_kafka_client, topic_store='test-store',
#                                       persistency_type=PersistencyType.MEMORY, serializer=store_builder_serializer,
#                                       loop=t_loop, rebuild=True)

test_store_manager = None

store_record_handler = StoreRecordHandler(test_store_manager)
store_builder_serializer.register_event_handler_store_record(StoreRecord, store_record_handler)


@pytest.yield_fixture()
def event_loop():
    loop = t_loop
    yield loop


@pytest.fixture
def get_local_memory_store_connection():
    return test_local_store_memory_persistency


@pytest.fixture
def get_global_memory_store_connection():
    return test_global_store_memory_persistency


@pytest.fixture
def get_avro_serializer():
    return test_serializer


@pytest.fixture
def get_avro_serializer_store():
    return test_serializer_local_store_memory_persistency, test_serializer_global_store_memory_persistency


@pytest.fixture
def get_assignor_kafka_client():
    return assignor_py_kafka_client


@pytest.fixture
def get_assignor_cluster_metadata():
    return assignor_cluster_metadata


@pytest.fixture
def get_store_manager():
    return test_store_manager
