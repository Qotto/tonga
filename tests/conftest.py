#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest

from aioevent.stores.local.memory import LocalStoreMemory
from aioevent.stores.globall.memory import GlobalStoreMemory


local_memory_store = LocalStoreMemory(name='local_store_memory_test')

global_memory_store = GlobalStoreMemory(name='global_store_memory_test')


@pytest.fixture
def get_local_memory_store_connection():
    return local_memory_store


@pytest.fixture
def get_global_memory_store_connection():
    return global_memory_store
