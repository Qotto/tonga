#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, Any, List

from tonga.models.events.base import BaseModel
from tonga.models.events.command.errors import CommandEventMissingProcessGuarantee


__all__ = [
    'BaseCommand',
]

PROCESSING_GUARANTEE: List[str] = ['at_least_once', 'at_most_once', 'exactly_once']


class BaseCommand(BaseModel):
    """A *command* is a record containing instructions to be processed by a service. The result of this processing
    is notified in a *result* record.

    Commands, whenever possible, should be idempotent_. It means that a
    command applied twice should have the same effect as a command applied once.

    Idempotence is not always possible. For instance, sending an e-mail cannot be idempotent: sending an e-mail once is
    not the same as sending it twice. Even if we try to deduplicate commands, there is no way, with usual SMTP servers,
    to send an e-mail and acknowledge the command in the same transaction.

    Commands have the same fields as records, and a few additional ones:

    processing_guarantee: one of
        - *at_least_once*: the command should be processed at least once. It is tolerated, in case of a failure, for it to be processed multiple times.
        - *at_most_once*: the command should be processed at most once. It is tolerated, in case of a failure, for it to not be processed at all.
        - *exactly_once*: the command should be processed exactly once. It is not tolerated, in case of a failure, for it to be processed more or less than once.

    Attributes:
        processing_guarantee (str): see class description for more details

    .. _idempotent:
       https://en.wikipedia.org/wiki/Idempotence

    """
    processing_guarantee: str

    def __init__(self, processing_guarantee: str = None, **kwargs):
        super().__init__(**kwargs)
        if processing_guarantee in PROCESSING_GUARANTEE:
            self.processing_guarantee = processing_guarantee
        else:
            raise CommandEventMissingProcessGuarantee

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        raise NotImplementedError

    @classmethod
    def event_name(cls) -> str:
        raise NotImplementedError
