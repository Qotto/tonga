#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Any, Type

from aioevent.model.base import BaseModel

__all__ = [
    'BaseSerializer',
]


class BaseSerializer:
    def __init__(self) -> None:
        pass

    def encode(self, event: BaseModel) -> bytes:
        raise NotImplementedError

    def decode(self, encoded_event: Any) -> BaseModel:
        raise NotImplementedError

    def register_class(self, event_class: Type[BaseModel], event_name: str) -> None:
        raise NotImplementedError
