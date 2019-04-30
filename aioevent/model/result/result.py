#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.app.base import BaseApp
from ..base import BaseModel

__all__ = [
    'BaseResult'
]


class BaseResult(BaseModel):
    error: Dict[str, Any]

    def __init__(self, error: Dict[str, Any] = None, **kwargs):
        super().__init__(**kwargs)
        self.error = error

    # This method is called when the result is received
    async def on_result(self, app: BaseApp, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        pass

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError
