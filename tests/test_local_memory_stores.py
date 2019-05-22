#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from aiokafka import TopicPartition

from aioevent.stores import BaseStoreMetaData
from aioevent.models.exceptions import StoreMetadataCantNotUpdated, StoreKeyNotFound, UninitializedStore


# Initialization test
def test_local_memory_store_set_store_position(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 0}
    current_instance = 2
    nb_replica = 4
    local_memory_store.set_store_position(current_instance, nb_replica, assigned_partitions, last_offsets)

    db_meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)
    assert local_memory_store.get_metadata().__dict__ == db_meta.__dict__


# Test raise UninitializedStore
def test_local_memory_uninitialized_store(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(UninitializedStore):
        local_memory_store.set('test', b'value')
    with pytest.raises(UninitializedStore):
        local_memory_store.get('test')
    with pytest.raises(UninitializedStore):
        local_memory_store.delete('test')
    with pytest.raises(UninitializedStore):
        local_memory_store.get_all()


# Test build store
def test_local_memory_store_build_set(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    local_memory_store.build_set('test', b'value')
    local_memory_store.build_set('test2', b'value')
    local_memory_store.build_set('test3', b'value')
    local_memory_store.build_delete('test2')

    local_memory_store.set_initialized(True)
    assert local_memory_store.is_initialized
    assert local_memory_store.get('test') == b'value'
    with pytest.raises(StoreKeyNotFound):
        local_memory_store.get('test2')
    assert local_memory_store.get('test3') == b'value'
    local_memory_store.delete('test')
    local_memory_store.delete('test3')


# Test set function
def test_local_memory_store_set(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    local_memory_store.set('test', b'value')
    assert local_memory_store.get('test') == b'value'


def test_local_memory_store_set_bad_key(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(ValueError):
        assert local_memory_store.set(b'test', b'value')


def test_local_memory_store_set_metadata(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(StoreMetadataCantNotUpdated):
        local_memory_store.set('metadata', b'value')


# Test get function
def test_local_memory_store_get(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    assert local_memory_store.get('test') == b'value'


def test_local_memory_store_get_not_found(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        local_memory_store.get('toto')


def test_local_memory_store_get_bad_key(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(ValueError):
        local_memory_store.get(b'toto')


# Test delete function
def test_local_memory_store_delete_not_found(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        local_memory_store.delete('toto')


def test_local_memory_store_delete_bad_key(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(ValueError):
        local_memory_store.delete(b'toto')


def test_local_memory_store_delete(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    local_memory_store.delete('test')
    with pytest.raises(StoreKeyNotFound):
        assert local_memory_store.get('test')


# Test get all
def test_local_memory_store_get_all(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    local_memory_store.set('test1', b'value1')
    local_memory_store.set('test2', b'value2')

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 0}
    current_instance = 2
    nb_replica = 4
    meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica)

    assert local_memory_store.get_all() == {'test1': b'value1', 'test2': b'value2',
                                            'metadata': bytes(str(meta.to_dict()), 'utf-8')}


# Test update_metadata_tp_offset function
def test_local_memory_get_metadata(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 0}
    current_instance = 2
    nb_replica = 4
    db_meta = local_memory_store.get_metadata().to_dict()
    local_meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica).to_dict()
    assert db_meta == local_meta


def test_local_memory_update_metadata_tp_offset(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    tp = TopicPartition('test', 2)
    local_memory_store.update_metadata_tp_offset(tp, 4)

    assigned_partitions = [TopicPartition('test', 2)]
    last_offsets = {TopicPartition('test', 2): 4}
    current_instance = 2
    nb_replica = 4
    local_meta = BaseStoreMetaData(assigned_partitions, last_offsets, current_instance, nb_replica).to_dict()

    db_meta = local_memory_store.get_metadata().to_dict()
    assert db_meta == local_meta
