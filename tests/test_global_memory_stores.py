#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from aiokafka import TopicPartition

from aioevent.stores import BaseStoreMetaData
from aioevent.models.exceptions import StoreMetadataCantNotUpdated, StoreKeyNotFound, UninitializedStore


# Initialization test
def test_global_memory_store_set_store_position(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 0}
    current_instance = 2
    nb_replica = 4
    global_memory_store.set_store_position(current_instance, nb_replica, assigned_partitions, last_offsets)

    db_meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)
    assert global_memory_store.get_metadata().__dict__ == db_meta.__dict__


# Test raise UninitializedStore
def test_global_memory_uninitialized_store(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(UninitializedStore):
        global_memory_store.get('test')
    with pytest.raises(UninitializedStore):
        global_memory_store.get_all()


# Test build store
def test_local_memory_store_build_set(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    global_memory_store.global_set('test', b'value')
    global_memory_store.global_set('test2', b'value')
    global_memory_store.global_set('test3', b'value')
    global_memory_store.global_delete('test2')

    global_memory_store.set_initialized(True)
    assert global_memory_store.is_initialized
    assert global_memory_store.get('test') == b'value'
    with pytest.raises(StoreKeyNotFound):
        global_memory_store.get('test2')
    assert global_memory_store.get('test3') == b'value'
    global_memory_store.global_delete('test')
    global_memory_store.global_delete('test3')


# Test set function
def test_global_memory_store__global_set(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    global_memory_store.global_set('test', b'value')
    assert global_memory_store.get('test') == b'value'


def test_global_memory_store_set_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(ValueError):
        assert global_memory_store.global_set(b'test', b'value')


def test_global_memory_store_global_set_metadata(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreMetadataCantNotUpdated):
        global_memory_store.global_set('metadata', b'value')

# Test get function


def test_global_memory_store_get(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    assert global_memory_store.get('test') == b'value'


def test_global_memory_store_get_not_found(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        global_memory_store.get('toto')


def test_global_memory_store_get_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(ValueError):
        global_memory_store.get(b'toto')

# Test delete function


def test_global_memory_store_global_delete_not_found(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        global_memory_store.global_delete('toto')


def test_global_memory_store_global_delete_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(ValueError):
        global_memory_store.global_delete(b'toto')


def test_global_memory_store_global_delete(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    global_memory_store.global_delete('test')
    with pytest.raises(StoreKeyNotFound):
        assert global_memory_store.get('test')

# Test get all


def test_global_memory_store_get_all(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    global_memory_store.global_set('test1', b'value1')
    global_memory_store.global_set('test2', b'value2')

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 0}
    current_instance = 2
    nb_replica = 4
    meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)

    assert global_memory_store.get_all() == {'test1': b'value1', 'test2': b'value2',
                                             'metadata': bytes(str(meta.to_dict()), 'utf-8')}

# Test update_metadata_tp_offset function


def test_global_memory_get_metadata(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 0}
    current_instance = 2
    nb_replica = 4
    local_meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica).to_dict()

    db_meta = global_memory_store.get_metadata().to_dict()
    assert db_meta == local_meta


def test_global_memory_update_metadata_tp_offset(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    tp = TopicPartition('test', 2)
    global_memory_store.update_metadata_tp_offset(tp, 4)

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 4}
    current_instance = 2
    nb_replica = 4
    local_meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica).to_dict()

    db_meta = global_memory_store.get_metadata().to_dict()
    assert db_meta == local_meta
