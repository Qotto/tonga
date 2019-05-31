#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import ast
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

    def __to_dict__(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'coffee_uuid': self.coffee_uuid,
            'amount': self.amount,
        }

    def __to_bytes_dict__(self) -> bytes:
        return str({
            'uuid': self.uuid,
            'coffee_uuid': self.coffee_uuid,
            'amount': self.amount,
            'is_paid': self.is_paid,
            'context': self.context
        }).encode('utf-8')

    @classmethod
    def __from_bytes_dict__(cls, data: bytes):
        dict_bill = ast.literal_eval(data.decode('utf-8'))
        bill = cls(coffee_uuid=dict_bill['coffee_uuid'], amount=dict_bill['amount'], uuid=dict_bill['uuid'])
        bill.set_is_paid(dict_bill['is_paid'])
        bill.set_context(dict_bill['context'])
        return bill
