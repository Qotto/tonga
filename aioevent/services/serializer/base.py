#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Any, Tuple, Union

from aioevent.models.events.base import BaseModel
from aioevent.models.handler.base import BaseHandler
from aioevent.models.store_record.base import BaseStoreRecord, BaseStoreRecordHandler

__all__ = [
    'BaseSerializer',
]


class BaseSerializer:
    def __init__(self) -> None:
        pass

    def encode(self, event: BaseModel) -> bytes:
        raise NotImplementedError

    def decode(self, encoded_event: Any) -> Tuple[Union[BaseModel, BaseStoreRecord],
                                                  Union[BaseHandler, BaseStoreRecordHandler]]:
        raise NotImplementedError

    def register_class(self, event_name: str, event_class: BaseModel, handler_class: BaseHandler) -> None:
        raise NotImplementedError
