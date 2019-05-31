#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aioevent.models.events.result.result import BaseResult

from typing import Dict, Any

__all__ = [
    'MakeCoffeeResult'
]


class MakeCoffeeResult(BaseResult):
    uuid: str

    def __init__(self, uuid: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.result.MakeCoffeeResult'
