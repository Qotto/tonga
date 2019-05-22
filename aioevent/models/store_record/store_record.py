#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any

from aioevent.models.store_record.base import BaseStoreRecord
__all__ = [
    'StoreRecord'
]


class StoreRecord(BaseStoreRecord):
    key: str
    type: str
    value: bytes

    def __init__(self, key: str, ttype: str, value: bytes, **kwargs) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.type = ttype
        self.value = value

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.store.record'
