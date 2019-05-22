#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any

from ..base import BaseModel

__all__ = [
    'BaseResult'
]


class BaseResult(BaseModel):
    error: Dict[str, Any]

    def __init__(self, error: Dict[str, Any] = None, **kwargs):
        super().__init__(**kwargs)
        self.error = error

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError
