#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, List, Union, Any

from ..models.bill import Bill


class CashRegister:
    total_in: float
    total_awaiting = float
    unpaid_bill = Dict[str, Bill]
    paid_bill = Dict[str, List[Union[Bill, Dict[str, Any]]]]

    def __init__(self):
        self.unpaid_bill = dict()
        self.paid_bill = dict()
        self.total_in = 0
        self.total_awaiting = 0

    def add_unpaid_bill(self, bill: Bill) -> None:
        self.total_awaiting += bill.amount
        self.unpaid_bill[bill.uuid] = bill

    def bill_paid(self, uuid: str, context: Dict[str, Any]):
        self.total_in += self.unpaid_bill[uuid].amount
        self.total_awaiting -= self.unpaid_bill[uuid].amount
        self.paid_bill[uuid][0] = self.unpaid_bill[uuid]
        self.paid_bill[uuid][1] = context
        del self.unpaid_bill[uuid]

    def how_mush_paid(self) -> float:
        return self.total_in

    def how_mush_awaiting(self) -> float:
        return self.total_awaiting
