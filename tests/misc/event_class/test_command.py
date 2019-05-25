#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aioevent.models.events.command.command import BaseCommand

from typing import Dict, Any

__all__ = [
    'TestCommand'
]


class TestCommand(BaseCommand):
    test: str

    def __init__(self, test: str, **kwargs):
        super().__init__(**kwargs)
        self.test = test

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.test.command'
