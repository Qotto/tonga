#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from tonga.models.events.result.result import BaseResult
from tonga.models.handlers.result.result_handler import BaseResultHandler


class TestResultHandler(BaseResultHandler):
    def __init__(self) -> None:
        pass

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.test.result'

    def on_result(self, event: BaseResult, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
