#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from tonga.models.events.result.result import BaseResult

from typing import Dict, Any

__all__ = [
    'TestResult'
]


class TestResult(BaseResult):
    test: str

    def __init__(self, test: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.test = test

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'tonga.test.result'
