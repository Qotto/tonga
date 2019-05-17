#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.services.stores.base import BaseStores, BaseStoreMetaData

__all_ = [
    'BaseGlobalStore',
]


class BaseGlobalStore(BaseStores):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get(self, key: str) -> Any:
        raise NotImplementedError

    def get_all(self) -> Dict[str, Any]:
        raise NotImplementedError

    def update_metadata_tp_offset(self, tp: TopicPartition, offset: int) -> None:
        raise NotImplementedError

    def _get_metadata(self) -> BaseStoreMetaData:
        raise NotImplementedError

    def _update_metadata(self) -> None:
        raise NotImplementedError
