#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest
from aiokafka import TopicPartition

# Import BaseGlobal / BaseLocal Stores
from tonga.stores.local_store.base import BaseLocalStore
from tonga.stores.global_store.base import BaseGlobalStore

# Import BaseStoreMetaData
from tonga.stores.base import BaseStoreMetaData

# Import kafka consumer & producer
from tonga.services.producer.kafka_producer import KafkaProducer
from tonga.services.consumer.kafka_consumer import KafkaConsumer

# Import exceptions
from tonga.errors import UninitializedStore, StoreKeyNotFound


@pytest.mark.asyncio
async def test_set_from_local_store_uninitialized_store_builder(get_store_manager):
    store_builder = get_store_manager

    with pytest.raises(UninitializedStore):
        await store_builder.set_from_local_store('test', b'value')


@pytest.mark.asyncio
async def test_get_from_local_store_uninitialized_store_builder(get_store_manager):
    store_builder = get_store_manager

    with pytest.raises(UninitializedStore):
        await store_builder.get_from_local_store('test')


@pytest.mark.asyncio
async def test_delete_from_local_store_uninitialized_store_builder(get_store_manager):
    store_builder = get_store_manager

    with pytest.raises(UninitializedStore):
        await store_builder.delete_from_local_store('test')


@pytest.mark.asyncio
async def test_get_from_global_store_uninitialized_store_builder(get_store_manager):
    store_builder = get_store_manager

    with pytest.raises(UninitializedStore):
        await store_builder.get_from_global_store('test')


def test_get_local_store_store_builder(get_store_manager):
    store_builder = get_store_manager

    local_store = store_builder.get_local_store()
    assert isinstance(local_store, BaseLocalStore)


def test_get_global_store_store_builder(get_store_manager):
    store_builder = get_store_manager

    global_store = store_builder.get_global_store()
    assert isinstance(global_store, BaseGlobalStore)


def test_get_current_instance_store_builder(get_store_manager):
    store_builder = get_store_manager

    current_instance = store_builder.get_current_instance()
    assert current_instance == 0


def test_get_nb_replica_store_builder(get_store_manager):
    store_builder = get_store_manager

    nb_replica = store_builder.get_nb_replica()
    assert nb_replica == 1


def test_get_is_event_sourcing_store_builder(get_store_manager):
    store_builder = get_store_manager

    event_sourcing = store_builder.is_event_sourcing()
    assert not event_sourcing


def test_get_producer(get_store_manager):
    store_builder = get_store_manager

    producer = store_builder.get_producer()
    assert isinstance(producer, KafkaProducer)


def test_get_consumer(get_store_manager):
    store_builder = get_store_manager

    consumer = store_builder.get_consumer()
    assert isinstance(consumer, KafkaConsumer)


@pytest.mark.skip(reason='Not yet testable')
@pytest.mark.asyncio
async def test_initialize_store_builder(get_store_manager):
    store_builder = get_store_manager

    await store_builder.initialize_store_builder()

    assigned_partitions = list()
    last_offsets = dict()
    assigned_partitions.append(TopicPartition('test-store', 0))
    last_offsets[TopicPartition('test-store', 0)] = 0
    test_store_metadata_local = BaseStoreMetaData(assigned_partitions, last_offsets, 0, 1)

    local_store = store_builder.get_local_store()
    local_store_metadata = await local_store.get_metadata()

    assert local_store_metadata.to_dict() == test_store_metadata_local.to_dict()

    assigned_partitions = list()
    last_offsets = dict()
    for i in range(0, 1):
        assigned_partitions.append(TopicPartition('test-store', 0))
    for j in range(0, 1):
        last_offsets[TopicPartition('test-store', 0)] = 0
    test_store_metadata_global = BaseStoreMetaData(assigned_partitions, last_offsets, 0, 1)

    global_store = store_builder.get_global_store()
    global_store_metadata = await global_store.get_metadata()

    assert global_store_metadata.to_dict() == test_store_metadata_global.to_dict()


@pytest.mark.asyncio
async def test_local_store_rebuild_store_builder(get_store_manager):
    store_builder = get_store_manager

    await store_builder.set_from_local_store_rebuild('test', b'value')
    await store_builder.set_from_local_store_rebuild('test1', b'value1')
    await store_builder.set_from_local_store_rebuild('test2', b'value2')

    await store_builder.delete_from_local_store_rebuild('test1')
    store_builder.set_local_store_initialize(True)

    assert await store_builder.get_from_local_store('test') == b'value'

    with pytest.raises(StoreKeyNotFound):
        await store_builder.get_from_local_store('test1')

    assert await store_builder.get_from_local_store('test2') == b'value2'


@pytest.mark.asyncio
async def test_global_store_rebuild_store_builder(get_store_manager):
    store_builder = get_store_manager

    await store_builder.set_from_global_store('test', b'value')
    await store_builder.set_from_global_store('test1', b'value1')
    await store_builder.set_from_global_store('test2', b'value2')

    await store_builder.delete_from_global_store('test1')
    store_builder.set_global_store_initialize(True)

    assert await store_builder.get_from_global_store('test') == b'value'

    with pytest.raises(StoreKeyNotFound):
        await store_builder.get_from_global_store('test1')

    assert await store_builder.get_from_global_store('test2') == b'value2'


@pytest.mark.skip(reason='Not yet testable')
@pytest.mark.asyncio
async def test_set_from_local_store_store_builder(get_store_manager):
    store_builder = get_store_manager

    await store_builder.set_from_local_store('test3', b'value3')
    assert await store_builder.get_from_local_store('test3') == b'value3'


@pytest.mark.skip(reason='Not yet testable')
@pytest.mark.asyncio
async def test_delete_from_local_store_store_builder(get_store_manager):
    store_builder = get_store_manager

    await store_builder.set_from_local_store('test4', b'value4')
    await store_builder.delete_from_local_store('test4')
    with pytest.raises(StoreKeyNotFound):
        await store_builder.get_from_local_store('test4')
