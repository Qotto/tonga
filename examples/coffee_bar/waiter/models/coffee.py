#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from uuid import uuid4
import ast

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

    def __to_dict__(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'cup_type': self.cup_type,
            'coffee_type': self.coffee_type,
            'coffee_for': self.coffee_for,
            'amount': self.amount
        }

    def __to_bytes_dict__(self) -> bytes:
        return str({
            'uuid': self.uuid,
            'cup_type': self.cup_type,
            'coffee_type': self.coffee_type,
            'coffee_for': self.coffee_for,
            'amount': self.amount,
            'state': self.state,
            'context': self.context
        }).encode('utf-8')

    @classmethod
    def __from_dict_bytes__(cls, data: bytes):
        dict_coffee = ast.literal_eval(data.decode('utf-8'))
        coffee = cls(coffee_type=dict_coffee['coffee_type'], cup_type=dict_coffee['cup_type'],
                     coffee_for=dict_coffee['coffee_for'], amount=dict_coffee['amount'], uuid=dict_coffee['uuid'])
        coffee.set_state(dict_coffee['state'])
        coffee.set_context(dict_coffee['context'])
        return coffee
