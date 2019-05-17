#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.app.aioevent import AioEvent
from aioevent.model.storage_builder.base import BaseStore
from aioevent.model.exceptions import KtableUnknownType

__all__ = [
    'StorageBuilder'
]


class StorageBuilder(BaseStore):
    key: str
    type: str
    value: Dict[str, Any]

    def __init__(self, key: bytes, ttype: str, value: Dict[str, Any], **kwargs) -> None:
        super().__init__(**kwargs)
        self.key = key.decode('utf-8')
        self.type = ttype
        self.value = value

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.storage.builder'

    async def local_state_handler(self, app: AioEvent, group_id: str, tp: TopicPartition, offset: int):
        if self.type == 'set':
            app.local_stores[self.store_name].set(self.key, self.__dict__)
        elif self.type == 'del':
            app.local_stores[self.store_name].delete(self.key)
        else:
            raise KtableUnknownType(f'Local state type: {self.type} is unknown', 500)
        app.local_stores[self.store_name].update_tp_offset(tp, offset)

    async def global_state_handler(self, app: AioEvent, group_id: str, tp: TopicPartition, offset: int):
        if self.type == 'set':
            app.global_stores[self.store_name].global_set(self.key, self.__dict__)
        elif self.type == 'del':
            app.global_stores[self.store_name].global_delete(self.key)
        else:
            raise KtableUnknownType(f'Global state type: {self.type} is unknown', 500)
        app.global_stores[self.store_name].update_tp_offset(tp, offset)
