#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aiokafka import TopicPartition

from typing import Union

from tonga.models.records.command.command import BaseCommand
from tonga.models.handlers.command.command_handler import BaseCommandHandler


class TestCommandHandler(BaseCommandHandler):
    def __init__(self) -> None:
        pass

    @classmethod
    def handler_name(cls) -> str:
        return 'tonga.test.command'

    def execute(self, event: BaseCommand, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:
        raise NotImplementedError
