#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from uuid import uuid4


class Coffee(object):
    uuid: str
    cup_type: str
    coffee_type: str
    coffee_for: str
    amount: float

    def __init__(self, coffee_type: str, cup_type: str, coffee_for: str, amount: float, uuid: str = None):
        self.coffee_type = coffee_type
        self.cup_type = cup_type
        self.coffee_for = coffee_for
        self.amount = amount

        if uuid is None:
            self.uuid = uuid4().hex
        else:
            self.uuid = uuid
