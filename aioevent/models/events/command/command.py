#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any, List

from ..base import BaseModel
from aioevent.models.exceptions import CommandEventMissingProcessGuarantee

__all__ = [
    'BaseCommand'
]

PROCESSING_GUARANTEE: List[str] = ['at_least_once', 'at_most_once', 'exactly_once']


class BaseCommand(BaseModel):
    processing_guarantee: str

    def __init__(self, processing_guarantee: str = None, **kwargs):
        super().__init__(**kwargs)
        if processing_guarantee in PROCESSING_GUARANTEE:
            self.processing_guarantee = processing_guarantee
        else:
            raise CommandEventMissingProcessGuarantee(f"Result Event need processing guarantee", 500)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError
