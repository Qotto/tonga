#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any, List

from tonga.stores import BaseStores, BaseStoreMetaData

__all_ = [
    'BaseGlobalStore',
]


class BaseGlobalStore(BaseStores):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def set_store_position(self, current_instance: int, nb_replica: int,
                                 assigned_partitions: List[TopicPartition],
                                 last_offsets: Dict[TopicPartition, int]) -> None:
        raise NotImplementedError

    def is_initialized(self) -> bool:
        raise NotImplementedError

    def set_initialized(self, initialized: bool) -> None:
        raise NotImplementedError

    async def get(self, key: str) -> Any:
        raise NotImplementedError

    async def get_all(self) -> Dict[str, bytes]:
        raise NotImplementedError

    async def global_set(self, key: str, value: bytes) -> None:
        raise NotImplementedError

    async def global_delete(self, key: str) -> None:
        raise NotImplementedError

    async def set_metadata(self, metadata: BaseStoreMetaData) -> None:
        raise NotImplementedError

    async def update_metadata_tp_offset(self, tp: TopicPartition, offset: int) -> None:
        raise NotImplementedError

    async def get_metadata(self) -> BaseStoreMetaData:
        raise NotImplementedError

    async def _update_metadata(self) -> None:
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError
