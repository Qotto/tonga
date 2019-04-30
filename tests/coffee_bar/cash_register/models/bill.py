#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from uuid import uuid4


class Bill:
    uuid: str
    coffee_uuid: str
    amount: float

    def __init__(self, coffee_uuid: str, amount: float, uuid: str = None):
        self.coffee_uuid = coffee_uuid
        self.amount = amount

        if uuid is None:
            self.uuid = uuid4().hex
        else:
            self.uuid = uuid
