#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from aioevent.models.events.result.result import BaseResult
from aioevent.models.handler.result.result_handler import BaseResultHandler


class TestResultHandler(BaseResultHandler):
    def __init__(self) -> None:
        pass

    @classmethod
    def handler_name(cls) -> str:
        return 'aioevent.test.result'

    def on_result(self, event: BaseResult, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
