#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from tonga.errors import StoreKeyNotFound, UninitializedStore
from tonga.stores.errors import BadEntryType


# Test raise UninitializedStore
@pytest.mark.asyncio
async def test_local_memory_uninitialized_store(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(UninitializedStore):
        await local_memory_store.get('test')
    with pytest.raises(UninitializedStore):
        await local_memory_store.set('test', b'test')
    with pytest.raises(UninitializedStore):
        await local_memory_store.delete('test')

# Test build store
@pytest.mark.asyncio
async def test_local_memory_store_build_set(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection

    await local_memory_store.__getattribute__('_build_set').__call__('test1', b'value1')
    await local_memory_store.__getattribute__('_build_set').__call__('test2', b'value2')
    await local_memory_store.__getattribute__('_build_set').__call__('test3', b'value3')

    await local_memory_store.__getattribute__('_build_delete').__call__('test2')

    local_memory_store.get_persistency().__getattribute__('_set_initialize').__call__()

    assert local_memory_store.get_persistency().is_initialize()

    assert await local_memory_store.get('test1') == b'value1'

    with pytest.raises(StoreKeyNotFound):
        await local_memory_store.get('test2')

    assert await local_memory_store.get('test3') == b'value3'


# Test set function
@pytest.mark.asyncio
async def test_local_memory_store_set(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection

    await local_memory_store.set('test4', b'value4')

    assert await local_memory_store.get('test4') == b'value4'


@pytest.mark.asyncio
async def test_local_memory_store_set_bad_key(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(BadEntryType):
        assert await local_memory_store.set(b'test5', b'value5')


# Test get function
@pytest.mark.asyncio
async def test_local_memory_store_get(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    assert await local_memory_store.get('test4') == b'value4'


@pytest.mark.asyncio
async def test_local_memory_store_get_not_found(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        await local_memory_store.get('toto')


@pytest.mark.asyncio
async def test_local_memory_store_get_bad_key(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(BadEntryType):
        await local_memory_store.get(b'toto')


# Test delete function
@pytest.mark.asyncio
async def test_local_memory_store_delete_not_found(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        await local_memory_store.delete('toto')


@pytest.mark.asyncio
async def test_local_memory_store_delete_bad_key(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection
    with pytest.raises(BadEntryType):
        await local_memory_store.delete(b'toto')


@pytest.mark.asyncio
async def test_local_memory_store_delete(get_local_memory_store_connection):
    local_memory_store = get_local_memory_store_connection

    await local_memory_store.set('test10', b'value10')

    await local_memory_store.delete('test10')

    with pytest.raises(StoreKeyNotFound):
        await local_memory_store.get('test10')
