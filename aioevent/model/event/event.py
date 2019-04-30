#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Dict, Any

from aioevent.app.base import BaseApp
from aioevent.model import BaseModel


class BaseEvent(BaseModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # This method is called when the result is received
    async def handle(self, app: BaseApp, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        pass

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError
