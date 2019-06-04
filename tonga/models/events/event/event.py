#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from typing import Dict, Any

from tonga.models import BaseModel


class BaseEvent(BaseModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError
