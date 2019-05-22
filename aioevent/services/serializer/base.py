#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Any, Type, Dict, Union

from aioevent.models.events.base import BaseModel
from aioevent.models.handler.base import BaseHandler

__all__ = [
    'BaseSerializer',
]


class BaseSerializer:
    def __init__(self) -> None:
        pass

    def encode(self, event: BaseModel) -> bytes:
        raise NotImplementedError

    def decode(self, encoded_event: Any) -> Dict[str, Union[BaseModel, BaseHandler]]:
        raise NotImplementedError

    def register_class(self, event_name: str, event_class: Type[BaseModel], handler_class: Type[BaseHandler]) -> None:
        raise NotImplementedError
