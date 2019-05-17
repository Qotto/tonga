#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from uuid import uuid4

from typing import Dict, Any


class Coffee(object):
    uuid: str
    cup_type: str
    coffee_type: str
    coffee_for: str
    amount: float
    state: str
    context: Dict

    def __init__(self, coffee_type: str, cup_type: str, coffee_for: str, amount: float, uuid: str = None):
        self.coffee_type = coffee_type
        self.cup_type = cup_type
        self.coffee_for = coffee_for
        self.amount = amount

        if uuid is None:
            self.uuid = uuid4().hex
        else:
            self.uuid = uuid

        self.state = 'ordered'
        self.context = dict()

    def set_state(self, state: str) -> None:
        self.state = state

    def set_context(self, context: str) -> None:
        self.context = context

    def __to_event_dict__(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'cup_type': self.cup_type,
            'coffee_type': self.coffee_type,
            'coffee_for': self.coffee_for,
            'amount': self.amount
        }
