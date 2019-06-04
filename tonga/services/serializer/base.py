#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Any, Tuple, Union, Type

from tonga.models.events.base import BaseModel
from tonga.models.handlers.base import BaseHandler
from tonga.models.store_record.base import BaseStoreRecord, BaseStoreRecordHandler

__all__ = [
    'BaseSerializer',
]


class BaseSerializer:
    def __init__(self) -> None:
        pass

    def encode(self, event: Any) -> Any:
        raise NotImplementedError

    def decode(self, encoded_event: Any) -> Any:
        raise NotImplementedError
