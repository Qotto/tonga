#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from uuid import uuid4

from typing import Dict, Any


class Bill:
    uuid: str
    coffee_uuid: str
    amount: float
    is_paid: bool
    context: Dict

    def __init__(self, coffee_uuid: str, amount: float, uuid: str = None):
        self.coffee_uuid = coffee_uuid
        self.amount = amount

        if uuid is None:
            self.uuid = uuid4().hex
        else:
            self.uuid = uuid

        self.is_paid = False
        self.context = dict()

    def set_is_paid(self, is_paid: bool) -> None:
        self.is_paid = is_paid

    def set_context(self, context: Dict[str, Any]) -> None:
        self.context = context

    def __dict_to_event__(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'coffee_uuid': self.coffee_uuid,
            'amount': self.amount,
        }
