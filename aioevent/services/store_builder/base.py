#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from aioevent.services.stores.local.base import BaseLocalStore
from aioevent.services.stores.globall.base import BaseGlobalStore

__all__ = [
    'BaseStoreBuilder'
]


class BaseStoreBuilder:
    def initialize_store_builder(self) -> None:
        raise NotImplementedError

    def get_local_store(self) -> BaseLocalStore:
        raise NotImplemented

    def get_global_store(self) -> BaseGlobalStore:
        raise NotImplemented

    def get_current_instance(self) -> int:
        raise NotImplemented

    def is_event_sourcing(self) -> bool:
        raise NotImplemented
