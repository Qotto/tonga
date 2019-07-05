#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from tonga.errors import StoreKeyNotFound, UninitializedStore
from tonga.stores.errors import BadEntryType


# Test raise UninitializedStore
@pytest.mark.asyncio
async def test_global_memory_uninitialized_store(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(UninitializedStore):
        await global_memory_store.get('test')


# Test build store
@pytest.mark.asyncio
async def test_global_memory_store_build_set(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    await global_memory_store.__getattribute__('_build_set').__call__('test1', b'value1')
    await global_memory_store.__getattribute__('_build_set').__call__('test2', b'value2')
    await global_memory_store.__getattribute__('_build_set').__call__('test3', b'value3')
    await global_memory_store.__getattribute__('_build_delete').__call__('test2')

    global_memory_store.get_persistency().__getattribute__('_set_initialize').__call__()

    assert global_memory_store.get_persistency().is_initialize()

    assert await global_memory_store.get('test1') == b'value1'

    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('test2')

    assert await global_memory_store.get('test3') == b'value3'

# Test set function
@pytest.mark.asyncio
async def test_global_memory_store_global_set(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    await global_memory_store.__getattribute__('_build_set').__call__('test4', b'value4')

    assert await global_memory_store.get('test4') == b'value4'


@pytest.mark.asyncio
async def test_global_memory_store_set_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection

    with pytest.raises(BadEntryType):
        await global_memory_store.__getattribute__('_build_set').__call__(b'test5', b'value5')


# Test get function
@pytest.mark.asyncio
async def test_global_memory_store_get(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    assert await global_memory_store.get('test4') == b'value4'


@pytest.mark.asyncio
async def test_global_memory_store_get_not_found(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('toto')


@pytest.mark.asyncio
async def test_global_memory_store_get_bad_key(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(BadEntryType):
        await global_memory_store.get(b'toto')


# Test delete function
@pytest.mark.asyncio
async def test_global_memory_store_global_delete_not_found(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.__getattribute__('_build_delete').__call__('toto')


@pytest.mark.asyncio
async def test_global_memory_store_global_delete_bad_entry(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    with pytest.raises(BadEntryType):
        await global_memory_store.__getattribute__('_build_delete').__call__(b'toto')


@pytest.mark.asyncio
async def test_global_memory_store_global_delete(get_global_memory_store_connection):
    global_memory_store = get_global_memory_store_connection
    await global_memory_store.__getattribute__('_build_delete').__call__('test4')
    with pytest.raises(StoreKeyNotFound):
        await global_memory_store.get('test4')
