#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from tonga.models.structs.positioning import KafkaPositioning
from tonga.stores.metadata.kafka_metadata import KafkaStoreMetaData
from tonga.errors import StoreMetadataCantNotUpdated, StoreKeyNotFound, UninitializedStore, BadKeyType


# Initialization test
@pytest.mark.asyncio
async def test_global_memory_store_set_store_position(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection

    assigned_partitions = [KafkaPositioning('test', 2, 0)]
    last_offsets = {KafkaPositioning.make_class_assignment_key('test', 2): KafkaPositioning('test', 2, 0)}
    current_instance = 2
    nb_replica = 4
    db_meta = KafkaStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)

    await global_memory_store.set_store_position(db_meta)

    r_db_meta = await global_memory_store.get_metadata()
    assert r_db_meta.to_dict() == db_meta.to_dict()


# Test raise UninitializedStore
@pytest.mark.asyncio
async def test_global_memory_uninitialized_store(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(UninitializedStore):
        await global_memory_store.get('test')
    with pytest.raises(UninitializedStore):
        await global_memory_store.get_all()


# Test build store
@pytest.mark.asyncio
async def test_global_memory_store_build_set(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    await global_memory_store.global_set('test', b'value')
    await global_memory_store.global_set('test2', b'value')
    await global_memory_store.global_set('test3', b'value')
    await global_memory_store.global_delete('test2')

    global_memory_store.set_initialized(True)
    assert global_memory_store.is_initialized
    assert await global_memory_store.get('test') == b'value'
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('test2')
    assert await global_memory_store.get('test3') == b'value'
    await global_memory_store.global_delete('test')
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('test')
    await global_memory_store.global_delete('test3')
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('test3')


# Test set function
@pytest.mark.asyncio
async def test_global_memory_store__global_set(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    await global_memory_store.global_set('test', b'value')
    assert await global_memory_store.get('test') == b'value'


@pytest.mark.asyncio
async def test_global_memory_store_set_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(BadKeyType):
        assert await global_memory_store.global_set(b'test', b'value')


@pytest.mark.asyncio
async def test_global_memory_store_global_set_metadata(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreMetadataCantNotUpdated):
        await global_memory_store.global_set('metadata', b'value')

# Test get function


@pytest.mark.asyncio
async def test_global_memory_store_get(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    assert await global_memory_store.get('test') == b'value'


@pytest.mark.asyncio
async def test_global_memory_store_get_not_found(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('toto')


@pytest.mark.asyncio
async def test_global_memory_store_get_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(BadKeyType):
        await global_memory_store.get(b'toto')


# Test delete function
@pytest.mark.asyncio
async def test_global_memory_store_global_delete_not_found(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.global_delete('toto')


@pytest.mark.asyncio
async def test_global_memory_store_global_delete_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(BadKeyType):
        await global_memory_store.global_delete(b'toto')


@pytest.mark.asyncio
async def test_global_memory_store_global_delete(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    await global_memory_store.global_delete('test')
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('test')


# Test get all
@pytest.mark.asyncio
async def test_global_memory_store_get_all(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection

    await global_memory_store.global_set('test1', b'value1')
    await global_memory_store.global_set('test2', b'value2')

    assigned_partitions = [KafkaPositioning('test', 2, 0)]
    last_offsets = {KafkaPositioning.make_class_assignment_key('test', 2): KafkaPositioning('test', 2, 0)}
    current_instance = 2
    nb_replica = 4
    meta = KafkaStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)

    assert await global_memory_store.get_all() == {'test1': b'value1', 'test2': b'value2',
                                                   'metadata': bytes(str(meta.to_dict()), 'utf-8')}


# Test update_metadata_tp_offset function
@pytest.mark.asyncio
async def test_global_memory_get_metadata(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection

    assigned_partitions = [KafkaPositioning('test', 2, 0)]
    last_offsets = {KafkaPositioning.make_class_assignment_key('test', 2): KafkaPositioning('test', 2, 0)}
    current_instance = 2
    nb_replica = 4
    meta = KafkaStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)

    db_meta = await global_memory_store.get_metadata()
    assert db_meta.to_dict() == meta.to_dict()


@pytest.mark.asyncio
async def test_global_memory_update_metadata_tp_offset(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    positioning = KafkaPositioning('test', 2, 4)
    await global_memory_store.update_metadata_tp_offset(positioning)

    assigned_partitions = [KafkaPositioning('test', 2, 0)]
    last_offsets = {KafkaPositioning.make_class_assignment_key('test', 2): KafkaPositioning('test', 2, 4)}
    current_instance = 2
    nb_replica = 4
    meta = KafkaStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)

    db_meta = await global_memory_store.get_metadata()
    assert db_meta.to_dict() == meta.to_dict()
