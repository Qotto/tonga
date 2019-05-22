#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any

from aioevent.models.store_record.base import BaseStoreRecord

__all__ = [
    'StoreRecord'
]


class StoreRecord(BaseStoreRecord):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.store.record'
